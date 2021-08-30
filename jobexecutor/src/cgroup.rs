use log::*;
use std::{ffi::OsStr, path::PathBuf};
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

        pub async fn create_child_cgroup(&self, pid: Pid) -> std::io::Result<ChildCGroup> {
            let path = self.parent_cgroup.join(pid.to_string());
            trace!("[{}] Creating child cgroup {:?}", pid, path);
            tokio::fs::create_dir(path.clone()).await.map_err(|err| {
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

        pub async fn clean_up(&self) -> std::io::Result<()> {
            debug!("Deleting {:?}", &self.path);
            // TODO low: this will fail if the child process forked
            tokio::fs::remove_dir(&self.path).await.map_err(|err| {
                error!("Cannot remove cgroup {:?} - {}", &self.path, err);
                err
            })
        }
    }
}

pub mod runtime {

    use super::{
        server_config::{CGroupConfig, ChildCGroup},
        *,
    };
    use anyhow::Context;
    use tokio::{fs::OpenOptions, io::AsyncWriteExt};

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
        pub async fn create_command<ITER, STR>(
            cgroup_config: &CGroupConfig,
            pid: Pid,
            program: &STR,
            args: ITER,
            limits: CGroupLimits,
        ) -> Result<(Command, ChildCGroup), CGroupCommandError>
        where
            ITER: ExactSizeIterator<Item = STR>,
            STR: AsRef<OsStr>,
        {
            let child_cgroup = cgroup_config
                .create_child_cgroup(pid)
                .await
                .map_err(CGroupCommandError::CGroupCreationFailed)?;
            trace!(
                "[{}] Configuring {:?} in cgroup {:?}",
                pid,
                limits,
                child_cgroup
            );
            let config_result = limits.write(&child_cgroup, cgroup_config).await;
            // if limit writing fails, clean up the child_cgroup
            match config_result {
                Ok(ok) => Ok(ok),
                Err(write_err) => {
                    // remove the cgroup, otherwise it will stay there forever
                    debug!("[{}] Cleaning up cgroup after failed configuration", pid);
                    child_cgroup
                        .clean_up()
                        .await
                        .with_context(|| {
                            format!(
                                "Failed to remove cgroup, as a result of failed, as a result of failed write - {}",
                                &write_err
                            )
                        })
                        .map_err(CGroupCommandError::WritingCGroupConfigurationFailed)?;
                    Err(CGroupCommandError::WritingCGroupConfigurationFailed(
                        write_err,
                    ))
                }
            }?;

            let mut command = Command::new(cgroup_config.cgexec_rs());
            // first argument is the cgroup name
            let mut new_args = Vec::with_capacity(args.len() + 2);
            new_args.push(child_cgroup.as_os_string().to_owned());
            // second is the program name
            new_args.push(program.as_ref().to_owned());
            new_args.extend(args.map(|item| item.as_ref().to_owned()));
            trace!("[{}] Running cgexec-rs with args {:?}", pid, new_args);
            command.args(new_args);
            Ok((command, child_cgroup))
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
        async fn write(
            &self,
            child_cgroup: &ChildCGroup,
            cgroup_config: &CGroupConfig,
        ) -> anyhow::Result<()> {
            if let Some(memory_max) = self.memory_max {
                Self::write_numeric_limit(child_cgroup, "memory.max", memory_max).await?;
            }
            if let Some(memory_swap_max) = self.memory_swap_max {
                Self::write_numeric_limit(child_cgroup, "memory.swap.max", memory_swap_max).await?;
            }
            if let Some(cpu_limit) = &self.cpu_limit {
                Self::write_limit(
                    child_cgroup,
                    "cpu.max",
                    format!(
                        "{} {}",
                        cpu_limit.cpu_max_quota_micros, cpu_limit.cpu_max_period_micros
                    ),
                )
                .await?;
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
                    Self::write_limit(child_cgroup, "io.max", io_max_buf).await?;
                }
            }
            Ok(())
        }

        async fn write_numeric_limit(
            child_cgroup: &ChildCGroup,
            file: &str,
            limit: u64,
        ) -> anyhow::Result<()> {
            Self::write_limit(child_cgroup, file, format!("{}\n", limit)).await
        }

        async fn write_limit<S: AsRef<str>>(
            child_cgroup: &ChildCGroup,
            file: &str,
            content: S,
        ) -> anyhow::Result<()> {
            let path = child_cgroup.as_path().join(file);
            let mut file = OpenOptions::new()
                .write(true)
                .open(&path)
                .await
                .with_context(|| format!("Cannot open {:?} for writing", path))
                .map_err(|err| {
                    // log the error
                    error!("{}", err);
                    err
                })?;
            let content = content.as_ref();
            let mut write_result = file.write_all(content.as_bytes()).await;

            if write_result.is_ok() {
                write_result = file.flush().await;
            }
            write_result
                .map_err(|err| {
                    // log the error + invalid content
                    error!(
                        "Error writing content \"{}\" to file {:?} - {}",
                        content, path, err
                    );
                    err
                })
                // do not store content in the error
                .with_context(|| format!("Cannot write to file {:?}", path))
        }
    }
}
