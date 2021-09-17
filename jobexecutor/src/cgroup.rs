use log::*;
use std::{ffi::OsStr, path::PathBuf};
use thiserror::Error;
use tokio::process::Command;

use crate::childinfo::Pid;

pub mod server_config {
    use std::path::Path;

    use super::*;

    /// Subfolder name for std::process::id() moving, see [CGroupConfig::move_current_process_to_subgroup]
    const SERVICE_SUBFOLDER_NAME: &str = "service";
    /// File name controlling which processes belong to a cgroup
    const CGROUP_PROCS: &str = "cgroup.procs";
    const CGROUP_SUBTREE_CONTROL: &str = "cgroup.subtree_control";
    const CGROUP_SUBTREE_CONTROL_ENABLE_CONTROLLERS: &str = "+cpu +memory +io";

    #[derive(Error, Debug)]
    pub enum CGroupConfigError {
        #[error("parent_cgroup must be an absolute path to a directory")]
        WrongParentCGroup,
        #[error("cgexec_rs must be an absolute path to a file")]
        WrongCGExecRs,
        #[error("file is in the way of service cgroup - {0}")]
        WrongServiceCGroup(PathBuf),
        #[error("cannot create service cgroup directory")]
        CannotCreateServiceCGroupDir(#[source] std::io::Error),
        #[error("cannot create service cgroup configuration")]
        CannotCreateServiceCGroup(#[source] concepts::CGroupWriteError),
    }

    #[derive(Debug)]
    pub struct CGroupConfigBuilder {
        pub parent_cgroup: PathBuf,
        pub cgexec_rs: PathBuf,
        pub cgroup_block_device_id: String,
        pub move_current_pid_to_subfolder: bool,
    }

    #[derive(Debug, Clone)]
    pub struct CGroupConfig {
        parent_cgroup: PathBuf,
        cgexec_rs: PathBuf,
        cgroup_block_device_id: String,
    }

    /// Global cgroup configuration.
    /// All new processes (that require cgroup limits) will live in subgroups of the parent_cgroup.
    /// Processes that use io.max will be limited only on one device stored in cgroup_block_device_id.
    impl CGroupConfig {
        pub async fn new(builder: CGroupConfigBuilder) -> Result<CGroupConfig, CGroupConfigError> {
            debug!("Configuring cgroup support: {:?}", builder);
            let parent_cgroup = builder.parent_cgroup;
            let cgexec_rs = builder.cgexec_rs;
            if !parent_cgroup.is_absolute() || !parent_cgroup.is_dir() {
                Err(CGroupConfigError::WrongParentCGroup)
            } else if !cgexec_rs.is_absolute() || !cgexec_rs.is_file() {
                Err(CGroupConfigError::WrongCGExecRs)
            } else {
                let cgroup_config = CGroupConfig {
                    parent_cgroup,
                    cgexec_rs,
                    cgroup_block_device_id: builder.cgroup_block_device_id,
                };
                if builder.move_current_pid_to_subfolder {
                    cgroup_config.move_current_process_to_subgroup().await?;
                }
                Ok(cgroup_config)
            }
        }

        pub fn cgexec_rs(&self) -> &PathBuf {
            &self.cgexec_rs
        }

        pub fn cgroup_block_device_id(&self) -> &str {
            &self.cgroup_block_device_id
        }

        pub async fn create_child_cgroup_folder(&self, pid: Pid) -> std::io::Result<ChildCGroup> {
            let path = self.parent_cgroup.join(pid.to_string());
            trace!("[{}] Creating child cgroup {:?}", pid, path);
            tokio::fs::create_dir(path.clone()).await.map_err(|err| {
                error!("[{}] Cannot create child cgroup {:?} - {}", pid, path, err);
                err
            })?;
            Ok(ChildCGroup { path })
        }

        /// When running as systemd service in a slice, create new subdirectory in
        /// the 'service' folder and move current process there.
        /// This is needed to follow the 'leaf rule' of cgroup v2.
        /// E.g. if the service lived in /sys/fs/cgroup/jobexecutor.slice/jobexecutor.service,
        /// it must be set as PARENT_CGROUP.
        /// Create 'service' subfolder if not present and move itself there.
        /// Add required controllers to PARENT_CGROUP/cgroup.subtree_control.
        /// Executed processes will inherit them as PARENT_CGROUP/$EID/cgroup.controllers.
        /// Adding limits to the server process is not required.
        async fn move_current_process_to_subgroup(&self) -> Result<(), CGroupConfigError> {
            let path = self.parent_cgroup.join(SERVICE_SUBFOLDER_NAME);
            if path.exists() && !path.is_dir() {
                return Err(CGroupConfigError::WrongServiceCGroup(path));
            }

            if !path.exists() {
                trace!("Creating service cgroup {:?}", path);
                tokio::fs::create_dir(path.clone()).await.map_err(|err| {
                    error!("Cannot create service cgroup {:?} - {}", path, err);
                    CGroupConfigError::CannotCreateServiceCGroupDir(err)
                })?;
            }
            // move current process to 'service' subgroup by writing to its 'cgroup.procs'
            let content = std::process::id().to_string();
            cgroup_writer::write_value_to_file(path.join(CGROUP_PROCS), content)
                .await
                .map_err(CGroupConfigError::CannotCreateServiceCGroup)?;

            // Make sure all needed controllers are in 'cgroup.subtree_control'
            // so that new cgroups can be limited.
            cgroup_writer::write_value_to_file(
                self.parent_cgroup.join(CGROUP_SUBTREE_CONTROL),
                CGROUP_SUBTREE_CONTROL_ENABLE_CONTROLLERS,
            )
            .await
            .map_err(CGroupConfigError::CannotCreateServiceCGroup)
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
            // TODO: this will fail if the child process forked
            // For production, forking/cloning might not be an issue
            // or killing of all processes in cgroup.procs might be needed.
            tokio::fs::remove_dir(&self.path).await.map_err(|err| {
                error!("Cannot remove cgroup {:?} - {}", &self.path, err);
                err
            })
        }
    }
}

pub mod runtime {
    use super::{
        concepts::{CGroupLimits, CGroupWriteError},
        server_config::{CGroupConfig, ChildCGroup},
        *,
    };

    #[derive(Error, Debug)]
    pub enum CGroupCommandError {
        #[error("[{0}] cannot create child cgroup")]
        CGroupCreationFailed(Pid, #[source] std::io::Error),
        #[error("[{0}] cannot remove child cgroup")]
        RemovingCGroupConfigurationFailed(Pid, #[source] CGroupWriteError, std::io::Error),
        #[error("[{0}] cannot configure child cgroup")]
        WritingCGroupConfigurationFailed(Pid, #[source] CGroupWriteError),
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
                .create_child_cgroup_folder(pid)
                .await
                .map_err(|err| CGroupCommandError::CGroupCreationFailed(pid, err))?;
            trace!(
                "[{}] Configuring {:?} in cgroup {:?}",
                pid,
                limits,
                child_cgroup
            );
            let config_result = cgroup_writer::write(&limits, &child_cgroup, cgroup_config).await;
            // if limit writing fails, clean up the child_cgroup
            match config_result {
                Ok(ok) => Ok(ok),
                Err(write_err) => {
                    // remove the cgroup, otherwise it will stay there forever
                    debug!("[{}] Cleaning up cgroup after failed configuration", pid);
                    let clean_up_result = child_cgroup.clean_up().await;
                    if let Err(clean_up_err) = clean_up_result {
                        warn!("Failed to remove cgroup, as a result of failed, as a result of failed write - {}", &write_err);
                        Err(CGroupCommandError::RemovingCGroupConfigurationFailed(
                            pid,
                            write_err,
                            clean_up_err,
                        ))
                    } else {
                        Err(CGroupCommandError::WritingCGroupConfigurationFailed(
                            pid, write_err,
                        ))
                    }
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
}

pub mod concepts {
    use super::*;

    #[derive(Error, Debug)]
    pub enum CGroupWriteError {
        #[error("cannot open cgroup file {0}")]
        CannotOpen(PathBuf, #[source] std::io::Error),
        #[error("cannot write cgroup file {0}")]
        CannotWrite(PathBuf, #[source] std::io::Error),
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
}

mod cgroup_writer {

    use super::server_config::{CGroupConfig, ChildCGroup};
    use super::*;
    use concepts::*;
    use std::path::Path;
    use tokio::{fs::OpenOptions, io::AsyncWriteExt};

    /// Write limits to various files inside child_cgroup folder.
    /// See https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html
    pub(crate) async fn write(
        limits: &CGroupLimits,
        child_cgroup: &ChildCGroup,
        cgroup_config: &CGroupConfig,
    ) -> Result<(), CGroupWriteError> {
        // consider disabling forking by setting pids.max=1, or bigger value to prevent fork bombs.
        // However this setting limits TIDs not PIDs.
        // Self::write_numeric_limit(child_cgroup, "pids.max", 1).await?;
        // Forking is currently not handled.
        if let Some(memory_max) = limits.memory_max {
            write_numeric_limit(child_cgroup, "memory.max", memory_max).await?;
        }
        if let Some(memory_swap_max) = limits.memory_swap_max {
            write_numeric_limit(child_cgroup, "memory.swap.max", memory_swap_max).await?;
        }
        if let Some(cpu_limit) = &limits.cpu_limit {
            write_limit(
                child_cgroup,
                "cpu.max",
                format!(
                    "{} {}",
                    cpu_limit.cpu_max_quota_micros, cpu_limit.cpu_max_period_micros
                ),
            )
            .await?;
        }
        if let Some(block_device_limit) = &limits.block_device_limit {
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
                io_max_buf.insert_str(0, &format!("{} ", cgroup_config.cgroup_block_device_id()));
                write_limit(child_cgroup, "io.max", io_max_buf).await?;
            }
        }
        Ok(())
    }

    async fn write_numeric_limit(
        child_cgroup: &ChildCGroup,
        file_name: &str,
        limit: u64,
    ) -> Result<(), CGroupWriteError> {
        write_limit(child_cgroup, file_name, format!("{}\n", limit)).await
    }

    async fn write_limit<S: AsRef<str>>(
        child_cgroup: &ChildCGroup,
        file_name: &str,
        content: S,
    ) -> Result<(), CGroupWriteError> {
        let path = child_cgroup.as_path().join(file_name);
        write_value_to_file(path, content).await
    }

    pub(crate) async fn write_value_to_file<S: AsRef<str>>(
        path: impl AsRef<Path>,
        content: S,
    ) -> Result<(), CGroupWriteError> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .await
            .map_err(|err| {
                error!("Cannot open {:?} for writing - {}", path, err);
                CGroupWriteError::CannotOpen(path.clone().to_path_buf(), err)
            })?;
        let content = content.as_ref();
        let mut write_result = file.write_all(content.as_bytes()).await;

        if write_result.is_ok() {
            write_result = file.flush().await;
        }
        write_result.map_err(|err| {
            error!(
                "Error writing content \"{}\" to file {:?} - {}",
                content, path, err
            );
            CGroupWriteError::CannotWrite(path.clone().to_path_buf(), err)
        })
    }
}
