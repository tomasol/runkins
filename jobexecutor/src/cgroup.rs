use log::*;
use std::{ffi::OsStr, fs, io, path::PathBuf};
use thiserror::Error;
use tokio::process::Command;

use crate::childinfo::Pid;

pub mod server_config {
    use std::path::Path;

    use super::*;

    #[derive(Error, Debug)]
    pub enum CGroupConfigError {
        #[error("parent_cgroup must be an absolute path to a directory")]
        WrongParentCGroup,
        #[error("cgexec_rs must be an absolute path to a file")]
        WrongCGExecRs,
    }

    #[derive(Debug)]
    pub struct CGroupConfigBuilder {
        pub parent_cgroup: PathBuf,
        pub cgexec_rs: PathBuf,
        pub cgroup_block_device_id: String,
    }

    #[derive(Debug, Clone)]
    pub struct CGroupConfig {
        parent_cgroup: PathBuf,
        cgexec_rs: PathBuf,
        cgroup_block_device_id: String,
    }

    impl CGroupConfig {
        pub fn new(builder: CGroupConfigBuilder) -> Result<CGroupConfig, CGroupConfigError> {
            let parent_cgroup = builder.parent_cgroup;
            let cgexec_rs = builder.cgexec_rs;
            if !parent_cgroup.is_absolute() || !parent_cgroup.is_dir() {
                Err(CGroupConfigError::WrongParentCGroup)
            } else if !cgexec_rs.is_absolute() || !cgexec_rs.is_file() {
                Err(CGroupConfigError::WrongCGExecRs)
            } else {
                Ok(CGroupConfig {
                    parent_cgroup,
                    cgexec_rs,
                    cgroup_block_device_id: builder.cgroup_block_device_id,
                })
            }
        }

        pub fn cgexec_rs(&self) -> &PathBuf {
            &self.cgexec_rs
        }

        pub fn cgroup_block_device_id(&self) -> &str {
            &self.cgroup_block_device_id
        }

        pub fn create_child_cgroup(&self, pid: Pid) -> io::Result<ChildCGroup> {
            let path = self.parent_cgroup.join(pid.to_string());
            trace!("[{}] Creating child cgroup {:?}", pid, path);
            fs::create_dir(path.clone()).map_err(|err| {
                error!("[{}] Cannot create child cgroup {:?} - {}", pid, path, err);
                err
            })?;
            Ok(ChildCGroup { path })
        }
    }

    #[derive(Debug)]

    pub struct ChildCGroup {
        // TODO: cgroup deletion on drop?
        path: PathBuf,
    }

    impl ChildCGroup {
        pub fn as_os_string(&self) -> &OsStr {
            self.path.as_os_str()
        }

        pub fn as_path(&self) -> &Path {
            self.path.as_path()
        }
    }
}

pub mod runtime {
    use super::{
        server_config::{CGroupConfig, ChildCGroup},
        *,
    };
    use anyhow::Context;
    use std::{fs::OpenOptions, io::Write};

    #[derive(Error, Debug)]
    pub enum CGroupCommandError {
        #[error("cannot create child cgroup - {0}")]
        CGroupCreationFailed(#[from] std::io::Error),
        #[error("cannot configure child cgroup - {0}")]
        WritingCGroupConfigurationFailed(#[from] anyhow::Error),
    }

    #[derive(Debug)]
    pub struct CGroupCommandFactory {}

    impl CGroupCommandFactory {
        /// Create new [`Command`] using program name and arguments.
        /// If cgroup_config is set to support cgroup, new cgroup will
        /// be created no matter if limits are provided or not.
        pub fn create_command<ITER, STR>(
            cgroup_config: &CGroupConfig,
            pid: Pid,
            program: &STR,
            args: ITER,
            limits: CGroupLimits,
        ) -> Result<Command, CGroupCommandError>
        where
            ITER: ExactSizeIterator<Item = STR>,
            STR: AsRef<OsStr>,
        {
            let child_cgroup_path = cgroup_config
                .create_child_cgroup(pid)
                .map_err(CGroupCommandError::CGroupCreationFailed)?;
            trace!(
                "[{}] Configuring {:?} in cgroup {:?}",
                pid,
                limits,
                child_cgroup_path
            );
            limits
                .write(&child_cgroup_path, cgroup_config)
                .map_err(CGroupCommandError::WritingCGroupConfigurationFailed)?;
            let mut command = Command::new(cgroup_config.cgexec_rs());
            // first argument is the cgroup name
            let mut new_args = Vec::with_capacity(args.len() + 2);
            new_args.push(child_cgroup_path.as_os_string().to_owned());
            // second is the program name
            new_args.push(program.as_ref().to_owned());
            new_args.extend(args.map(|item| item.as_ref().to_owned()));
            trace!("[{}] Running cgexec-rs with args {:?}", pid, new_args);
            command.args(new_args);
            Ok(command)
        }
    }

    #[derive(Debug)]
    pub struct CpuLimit {
        pub cpu_max_quota_micros: u64,
        pub cpu_max_period_micros: u64,
    }

    #[derive(Debug)]
    pub struct BlockDeviceLimit {
        pub io_max_rbps: Option<u64>,
        pub io_max_riops: Option<u64>,
        pub io_max_wbps: Option<u64>,
        pub io_max_wiops: Option<u64>,
    }

    #[derive(Debug, Default)]
    pub struct CGroupLimits {
        pub memory_max: Option<u64>,
        pub memory_swap_max: Option<u64>,
        pub cpu_limit: Option<CpuLimit>,
        pub block_device_limit: Option<BlockDeviceLimit>,
    }

    impl CGroupLimits {
        /// Write limits to various files inside child_cgroup folder.
        /// See https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html
        fn write(
            &self,
            child_cgroup: &ChildCGroup,
            cgroup_config: &CGroupConfig,
        ) -> anyhow::Result<()> {
            if let Some(memory_max) = self.memory_max {
                Self::write_numeric_limit(child_cgroup, "memory.max", memory_max)?;
            }
            if let Some(memory_swap_max) = self.memory_swap_max {
                Self::write_numeric_limit(child_cgroup, "memory.swap.max", memory_swap_max)?;
            }
            if let Some(cpu_limit) = &self.cpu_limit {
                Self::write_limit(
                    child_cgroup,
                    "cpu.max",
                    format!(
                        "{} {}",
                        cpu_limit.cpu_max_quota_micros, cpu_limit.cpu_max_period_micros
                    ),
                )?;
            }
            if let Some(block_device_limit) = &self.block_device_limit {
                let mut io_max_buf = String::new();
                if let Some(num) = block_device_limit.io_max_rbps {
                    io_max_buf += &format!("rbps={} ", num);
                }
                if let Some(num) = block_device_limit.io_max_riops {
                    io_max_buf += &format!("riops={} ", num);
                }
                if let Some(num) = block_device_limit.io_max_wbps {
                    io_max_buf += &format!("wbps={} ", num);
                }
                if let Some(num) = block_device_limit.io_max_wiops {
                    io_max_buf += &format!("wiops={} ", num);
                }
                if !io_max_buf.is_empty() {
                    io_max_buf
                        .insert_str(0, &format!("{} ", cgroup_config.cgroup_block_device_id()));
                    Self::write_limit(child_cgroup, "io.max", io_max_buf)?;
                }
            }
            Ok(())
        }

        fn write_numeric_limit(
            child_cgroup: &ChildCGroup,
            file: &str,
            limit: u64,
        ) -> anyhow::Result<usize> {
            Self::write_limit(child_cgroup, file, format!("{}\n", limit))
        }

        fn write_limit<S: AsRef<str>>(
            child_cgroup: &ChildCGroup,
            file: &str,
            content: S,
        ) -> anyhow::Result<usize> {
            let path = child_cgroup.as_path().join(file);
            let mut file = OpenOptions::new()
                .write(true)
                .open(&path)
                .with_context(|| format!("Cannot open {:?} for writing", path))
                .map_err(|err| {
                    error!("{}", err);
                    err
                })?;
            let content = content.as_ref();
            file.write(content.as_bytes())
                .map_err(|err| {
                    error!(
                        "Error writing content {} to file {:?} - {}",
                        content, path, err
                    );
                    err
                })
                .with_context(|| format!("Cannot write to file {:?}", path))
        }
    }
}
