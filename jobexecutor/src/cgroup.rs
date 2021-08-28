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
    }

    /// Avoid flipping the arguments in fn new
    #[derive(Debug, Clone)]
    pub struct CGroupConfig {
        parent_cgroup: PathBuf,
        cgexec_rs: PathBuf,
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
                })
            }
        }

        pub fn cgexec_rs(&self) -> &PathBuf {
            &self.cgexec_rs
        }

        pub fn create_child_cgroup(&self, pid: Pid) -> io::Result<ChildCGroup> {
            let path = self.parent_cgroup.join(pid.to_string());
            trace!("[{}] Creating child cgroup {:?}", pid, path);
            fs::create_dir(path.clone())?;
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
        pub fn create_command<I, S>(
            cgroup_config: &CGroupConfig,
            pid: Pid,
            program: &S,
            args: I,
            limits: CGroupLimits,
        ) -> Result<Command, CGroupCommandError>
        where
            I: ExactSizeIterator<Item = S>,
            S: AsRef<OsStr>,
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
                .write(&child_cgroup_path)
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

    // #[derive(Debug)]
    // pub struct CpuLimit {
    //     pub cpu_max_quota_micros: u64,
    //     pub cpu_max_period_micros: u64,
    // }

    // TODO BlockDeviceLimit

    #[derive(Debug, Default)]
    pub struct CGroupLimits {
        pub memory_max: Option<u64>,
        pub memory_swap_max: Option<u64>,
        // pub cpu_limit: Option<CpuLimit>,
    }

    impl CGroupLimits {
        fn write(&self, child_cgroup: &ChildCGroup) -> anyhow::Result<()> {
            if let Some(memory_max) = self.memory_max {
                Self::write_numeric_limit(child_cgroup, "memory.max", memory_max)?;
            }
            if let Some(memory_swap_max) = self.memory_swap_max {
                Self::write_numeric_limit(child_cgroup, "memory.swap.max", memory_swap_max)?;
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
                .with_context(|| format!("Cannot open {:?} for writing", path))?;

            file.write(content.as_ref().as_bytes())
                .with_context(|| format!("Cannot write {:?}", path))
        }
    }
}
