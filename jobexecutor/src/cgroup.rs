use log::*;
use std::{ffi::OsStr, fs, io, path::PathBuf};
use thiserror::Error;
use tokio::process::Command;

use crate::childinfo::Pid;

#[derive(Error, Debug)]
pub enum CGroupValidationError {
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
    pub fn new(builder: CGroupConfigBuilder) -> Result<CGroupConfig, CGroupValidationError> {
        let parent_cgroup = builder.parent_cgroup;
        let cgexec_rs = builder.cgexec_rs;
        if !parent_cgroup.is_absolute() || !parent_cgroup.is_dir() {
            Err(CGroupValidationError::WrongParentCGroup)
        } else if !cgexec_rs.is_absolute() || !cgexec_rs.is_file() {
            Err(CGroupValidationError::WrongCGExecRs)
        } else {
            Ok(CGroupConfig {
                parent_cgroup,
                cgexec_rs,
            })
        }
    }

    pub fn generate_new_cgroup(&self, name: String) -> io::Result<PathBuf> {
        let path = self.parent_cgroup.join(name);
        fs::create_dir(path.clone())?;
        Ok(path)
    }

    // TODO: cgroup deletion
}

#[derive(Debug)]
pub struct CGroupCommandWrapper {
    cgroup_config: Option<CGroupConfig>,
}

impl CGroupCommandWrapper {
    pub fn new(cgroup_config: Option<CGroupConfig>) -> CGroupCommandWrapper {
        CGroupCommandWrapper { cgroup_config }
    }

    pub fn command<I, S>(&self, pid: Pid, program: &S, args: I) -> Command
    where
        I: ExactSizeIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        if self.cgroup_config.is_some() {
            let cgroup_config = self.cgroup_config.clone().unwrap();
            let mut command = Command::new(cgroup_config.cgexec_rs);
            // first argument is the cgroup name
            let mut new_args = Vec::with_capacity(args.len() + 2);
            // TODO not the parent!
            new_args.push(cgroup_config.parent_cgroup.as_os_str().to_owned());
            // second is the program name
            new_args.push(program.as_ref().to_owned());
            new_args.extend(args.map(|item| item.as_ref().to_owned()));
            trace!("[{}] Running cgexec-rs with args {:?}", pid, new_args);
            command.args(new_args);
            command
        } else {
            let mut command = Command::new(program);
            command.args(args);
            command
        }
    }
}
