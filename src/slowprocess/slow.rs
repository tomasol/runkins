use std::{
    thread,
    time::{Duration, SystemTime},
};

fn main() {
    loop {
        println!("{:?}", SystemTime::now());
        thread::sleep(Duration::from_secs(1));
    }
}
