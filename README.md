# Runkins

Folder structure:
* cli - binary `runkins` - CLI
* lib - library used by the server, `examples/slow` binary for testing
* proto - `*.proto` definition
* server - binary `runkins-server` - gRPC server
* systemd - sample .service file

## Building
### Requirements
* Rust 2021 edition

For cgroup functionality (required when running a process with limits set):
* cgroup v2 enabled
* `systemd --version` >= 244 for cgroup v2 controller delegation to non-root users. This can be worked around e.g. by running as root.

More information can be found in the **cgroup** section.

### cargo build
To create a debug build of all components run
```sh
cargo build
```

Alternatively run with ` --release` to get the release build.

## Running

To see the error logs in the terminal, use
```sh
export RUST_LOG=runkins_server=debug,runkins_lib=debug,info
 ```

To run the server, execute:
```sh
cargo run --bin runkins-server
```
This will start the gRPC server on (currently hardcoded)
`localhost:50051`. The CLI has the same hardcoded address.


### CLI
Each RPC is be executed as a separate CLI subcommand. To start
a process, run
```sh
cargo run --bin runkins start -- ls -la
```
The `start` subcommand outputs the Execution ID to stdout.
All other subcommands (`status`, `stop`, `output`, `remove`) use it as an argument.
Example workflow:
```sh
EID=$(cargo run --bin runkins start -- ls -la)
cargo run --bin runkins status $EID
cargo run --bin runkins output $EID
cargo run --bin runkins stop $EID
cargo run --bin runkins remove $EID
```
To get help with commands, use `--help` flag.

### Installing the CLI
```sh
cargo install --bin runkins --path cli
```

## Testing
### Manual testing
Start the server, then execute the binary
[slow](jobexecutor/examples/slow.rs) created just for testing:
```sh
EID=$(cargo run --bin runkins start -- \
 cargo run --example slow 10)
```
Test that the system works as expected. The `output` subcommand
should write lines to stdout and stderr. After the program
writes 10 lines, the clients should disconnect and the status
should be `Exited with code 0`.

To test different exit codes,
use `slow 1 3`, which will end with status `Exited with code 3`.

Killing the `slow` process using `killall slow` or `stop` RPC should end with status `Exited with signal`.

### Automated testing
Run
```sh
cargo test
```
To execute tests that are dependent on cgroup v2 and systemd-run, enable `test_systemd_run` configuration
either via `RUST_FLAGS` or `.cargo/config`.

## cgroup support
Please read [cgexec-rs](cgexec-rs/README.md) and verify it is working as described.

Following environment variables need to be set, otherwise cgroup support will not be enabled.
* PARENT_CGROUP - full path to a cgroup directory that can be written by the current user
* CGROUP_BLOCK_DEVICE_ID - in form of MAJOR:MINOR, see `lsblk`
* CGEXEC_RS - path to cgexec-rs binary, might be omitted if it lives in the same folder as `jobexecutor-server`
* CGROUP_MOVE_CURRENT_PID_TO_SUBFOLDER_ENABLED - required for systemd service support. If set (to anything), creates PARENT_CGROUP/service cgroup if needed and moves current pid there. This setting also adds the required controllres to $PARENT_CGROUP/cgroup.subtree_control .

### Starting new user slice using systemd-run

If the parent cgroup is created using `systemd-run`, make sure the shell is still open. Example:
```sh
$ systemd-run --user -p Delegate=yes --slice=my.slice --shell
# depending on the user, path might be:
# export PARENT_CGROUP=/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/my.slice
```

### Managing the slice and service using systemd .service file
Create file `jobexecutor.service` based on `systemd/jobexecutor-sample.service` and
move/link it to `/etc/systemd/system/` folder. Then start the service:
```sh
systemctl daemon-reload
systemctl start jobexecutor
```

### Verification
Verify that the process runs in its own cgroup:
```sh
# note the --limits flag - if not set, cgroup will not be created
$ EID=$(cargo run --bin runkins start --limits -- cat /proc/self/cgroup)
$ cargo run --bin runkins output $EID
0::/user.slice/user-1000.slice/user@1000.service/my.slice/15395846019127741322
$ cargo run --bin runkins remove $EID
```

Verify that all required controllers `cpu io memory` are available:
```sh
$ EID=$(cargo run --bin runkins start --limits -- \
 sh -c 'cat /sys/fs/cgroup/$(cat /proc/self/cgroup | cut -d ':' -f 3)/cgroup.controllers')
$ cargo run --bin runkins output $EID
cpu io memory pids
$ cargo run --bin runkins remove $EID
```

### Setting cgroup limits via CLI
Switches for limits can be discovered using help.
```sh
$ cargo run --bin runkins start --help
runkins-start 0.1.0

USAGE:
    runkins start [FLAGS] [OPTIONS] <path> [args]...

FLAGS:
    -h, --help       Prints help information
    -l, --limits     Enable cgroup limits
    -V, --version    Prints version information

OPTIONS:
        --cpu-max-period-micros <cpu-max-period-micros>    Set cpu.max, period part, both parts must be set together
        --cpu-max-quota-micros <cpu-max-quota-micros>      Set cpu.max, quota part, both parts must be set together
        --io-max-rbps <io-max-rbps>                        Set io.max, rbps value
        --io-max-riops <io-max-riops>                      Set io.max, riops value
        --io-max-wbps <io-max-wbps>                        Set io.max, wbps value
        --io-max-wiops <io-max-wiops>                      Set io.max, wiops value
        --memory-max <memory-max>                          Set memory.max in bytes
        --memory-swap-max <memory-swap-max>                Set memory.swap.max in bytes

ARGS:
    <path>
    <args>...
```
Note that `--limits` flag must be set, otherwise the CLI will complain.

Example:
```sh
# this should succeed:
$ EID=$(cargo run --bin runkins start \
 --limits --memory-max 1000000 --memory-swap-max 0 -- \
  bash --help)
# this should exit with signal:
$ EID=$(cargo run --bin runkins start \
 --limits --memory-max 100000 --memory-swap-max 0 -- \
  bash --help)
```

## mTLS
Currently not implemented.
