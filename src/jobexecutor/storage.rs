use std::collections::HashMap;

/*
Types:
pid: 32 bit - https://stackoverflow.com/questions/1922761/size-of-pid-t-uid-t-gid-t-on-linux#1922892
exit status: 8 bit - https://unix.stackexchange.com/questions/504663/what-is-linux-exit-status-and-list-of-all-status
signal count:32 + real time signals, fits into 8 bit - https://linux.die.net/man/7/signal
*/
struct ProcessStatus {
    exit_status: Option<u8>,
    with_signal: Option<u8>,
}

struct ExecutionId(String);

struct PID(u32);

struct ExecutionState {
    stdout: String,
    stderr: String,
    process_status: ProcessStatus,
}

trait Storage {
    fn create(&mut self, path: impl Into<String>, args: Vec<String>, pid: PID) -> ExecutionId;
    fn find(&self, id: ExecutionId) -> Option<ExecutionState>;
}

struct InMemoryStorage {
    map: HashMap<String, ExecutionState>,
}

impl InMemoryStorage {
    fn new() -> InMemoryStorage {
        InMemoryStorage {
            map: HashMap::new(),
        }
    }
}

impl Storage for InMemoryStorage {
    fn create(&mut self, path: impl Into<String>, args: Vec<String>, pid: PID) -> ExecutionId {}

    fn find(&self, id: ExecutionId) -> Option<ExecutionState> {
        todo!()
    }
}
