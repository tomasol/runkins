use std::{env, thread, time::Duration};

fn main() {
    let mut i: usize = 0;
    let mut max = None;
    let args: Vec<String> = env::args().collect();
    if args.len() == 2 {
        max = args[1].parse::<usize>().ok();
    }

    loop {
        if i % 2 == 0 {
            println!("{}", i);
        } else {
            eprintln!("{}", i);
        }
        i += 1;
        if max == Some(i) {
            break;
        }
        thread::sleep(Duration::from_secs(1));
    }
}
