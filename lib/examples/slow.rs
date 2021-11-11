use std::{env, os::unix::prelude::AsRawFd, thread, time::Duration};

/// Small binary that writes to stdout/stderr once per second.
/// Accepts one numerical argument that specifies the max number of iterations.
/// If no arguments are provided, will loop forever.
fn main() {
    let mut i: usize = 0;
    let args: Vec<String> = env::args().collect();
    let max_iterations = if args.len() > 1 {
        args[1].parse::<usize>().ok()
    } else {
        None
    };
    let exit_code = if args.len() == 3 {
        args[2].parse::<i32>().unwrap()
    } else {
        0
    };

    loop {
        if i % 2 == 0 {
            println!("{}", i);
            if Some(i + 2) == max_iterations {
                println!("closing stdout");
                unsafe {
                    libc::close(std::io::stdout().as_raw_fd());
                }
            }
        } else {
            eprintln!("{}", i);
            if Some(i + 2) == max_iterations {
                eprintln!("closing stderr");
                unsafe {
                    libc::close(std::io::stderr().as_raw_fd());
                }
            }
        }
        i += 1;
        if max_iterations == Some(i) {
            break;
        }
        thread::sleep(Duration::from_secs(1));
    }
    std::process::exit(exit_code);
}
