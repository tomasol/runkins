# jobexecutor

Folder structure:
* cgexec-rs - binary `cgexec-rs` - based on `cgexec` [doc](https://linux.die.net/man/1/cgexec) [src](https://github.com/libcgroup/libcgroup/blob/main/src/tools/cgexec.c), used by the server
* cli - binary `jobexecutor-cli` - CLI
* jobexecutor - library used by the server
* proto - `*.proto` definition
* server - binary `jobexecutor-server` - gRPC server
* testing - binary `slow` for testing

## Building
### Requirements
* Minimum Supported Rust Version: 1.54.0

For cgroup functionality (required when running a process with limits set):
* cgroup v2 enabled
* `systemd --version` >= 244 for cgroup v2 controller delegation to non-root users. This can be worked around e.g. by running as root.

More information can be found in **cgroup** section and in [cgexec-rs](cgexec-rs/README.md) .
### cargo build
To create a debug build of all components run
```sh
cargo build
```

Alternatively run with ` --release` to get the release build.

## Running
To run the server, execute:
```sh
cargo run --bin jobexecutor-server
```
This will start the gRPC server on (currently hardcoded)
`localhost:50051`. The CLI has the same hardcoded address.

To see the error logs in the console, use
```sh
RUST_LOG=jobexecutor_server=trace,jobexecutor=trace \
 cargo run --bin jobexecutor-server
 ```

### CLI
Each RPC is be executed as a separate CLI subcommand. To start
a process, run
```sh
cargo run --bin jobexecutor-cli start -- ls -la
```
The `start` subcommand outputs the Execution ID to stdout.
All other subcommands (`status`, `stop`, `output`, `remove`) use it as an argument.
Example workflow:
```sh
EID=$(cargo run --bin jobexecutor-cli start -- ls -la)
cargo run --bin jobexecutor-cli status $EID
cargo run --bin jobexecutor-cli output $EID
cargo run --bin jobexecutor-cli stop $EID
cargo run --bin jobexecutor-cli remove $EID
```
To get help with commands, use `--help` flag.

## Testing
### Manual testing
Start the server, then execute the binary
[slow](testing/src/slow.rs) created just for testing:
```sh
EID=$(cargo run --bin jobexecutor-cli start -- \
 cargo run --bin slow 10)
```
Test that the system works as expected. The `output` subcommand
should write lines to stdout and stderr. After the program
writes 10 lines, the clients should disconnect and the status
should be `Exited with code 0`.

To test different exit codes,
use `slow 1 3`, which will end with status `Exited with code 3`.

Killing the `slow` process using `killall slow` should end with status `Exited with signal`.


### Automated testing
Not implemented yet.


## cgroup
Currently not implemented.

To check that cgroup v2 is enabled and the required controllers (`memory`, `cpu`, `io`)
can be delegated, follow this [guide](https://rootlesscontaine.rs/getting-started/common/cgroup2/)

To manually create a cgroup:
```sh
mkdir /sys/fs/cgroup/cg1
echo 0 > /sys/fs/cgroup/cg1/memory.swap.max
echo 10000000 > /sys/fs/cgroup/cg1/memory.max
echo $PID >> /sys/fs/cgroup/cg1/cgroup.procs
```
To use `cgexec-rs`:
```sh
./target/debug/cgexec-rs /sys/fs/cgroup/cg1 sleep infinity
```

TODO:
* permissions
* creation
* cleanup (kill all pids, then cgroup)

## mTLS
Currently not implemented.
