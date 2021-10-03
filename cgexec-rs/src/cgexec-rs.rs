use anyhow::bail;
use anyhow::{Context, Result};
use std::env;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::process;

// TODO: remove
/*
https://docs.rs/tokio/1.12.0/tokio/process/struct.Command.html#method.pre_exec
https://fasterthanli.me/articles/a-terminal-case-of-linux
*/

fn attach_process_to_cgroup(pid: u32, cgroup_string: &str) -> Result<()> {
    // TODO parse args using structopt
    let cgroup_path = Path::new(cgroup_string);

    if !cgroup_path.is_absolute() {
        bail!(
            "cgroup {:?} must be absolute path, try prefixing with \
             '/sys/fs/cgroup/' or find the mount point by running \
             `mount -l | grep cgroup2`",
            cgroup_path
        );
    }

    if !cgroup_path.is_dir() {
        bail!("Not a directory: {:?}", cgroup_path);
    }

    let cgroup_procs_path = cgroup_path.join("cgroup.procs");
    let mut cgroup_procs = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&cgroup_procs_path)
        .with_context(|| format!("Cannot open {:?}", cgroup_procs_path))?;

    cgroup_procs
        .seek(SeekFrom::End(0))
        .with_context(|| format!("Cannot seek {:?}", cgroup_procs_path))?;
    cgroup_procs
        .write(format!("{}\n", pid).as_bytes())
        .with_context(|| format!("Cannot write {:?}", cgroup_procs_path))?;
    cgroup_procs.flush()?;
    Ok(())
}

fn main() -> Result<()> {
    let mut args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 2 {
        bail!("Must specify cgroup and command to execute");
    }
    // attach current process to this cgroup
    attach_process_to_cgroup(process::id(), &args.remove(0))?;
    let program = args.remove(0);
    // calls libc's execvp underneath
    // see https://linux.die.net/man/3/execvp
    let err = exec::Command::new(&program).args(&args).exec();
    // Exec the specified program.  If all goes well, this will never
    // return.  If it does return, it will always retun an error.
    Err(err.into())
}
