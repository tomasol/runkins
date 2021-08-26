# cgexec-rs
Switches to the target cgroup and `exec`s the supplied command.
Based on libcgroup's cgexec.

## Usage
```sh
cgexec absolute-path-to-target-cgroup command <args>
```

## Running
The binary can be run either using `cargo run --bin cgexec-bin` or
executing the binary in `target` folder if it was already built by cargo.

### Setting up cgroup v2 with systemd-run
In order to manage cgroup by non-root users,
[enable controller delegation](https://rootlesscontaine.rs/getting-started/common/cgroup2/)

Verify that the delegation is enabled:
```sh
$ cat /sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service/cgroup.controllers
```
The needed controllers are `cpu memory io` . Follow the guide above if any of them are missing.

## Testing
We are assuming that the unified cgroup hierarchy is mouted at `/sys/fs/cgroup`.

Start new shell in a new systemd slice:
```sh
$ systemd-run --user -p Delegate=yes --slice=my.slice --shell
```

Inside this shell, examine the current cgroup:
```sh
$ cat /proc/self/cgroup
0::/user.slice/user-1000.slice/user@1000.service/my.slice/run-u690.service
$ PARENT_CGROUP=/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/my.slice
```

The shell in this example lives in the leaf `run-u690.service`.

Create new cgroup `TARGET_CGROUP` under the `PARENT_CGROUP`, verify all required controllers are available:
```sh
$ TARGET_CGROUP=$PARENT_CGROUP/temporary
$ mkdir $TARGET_CGROUP
$ cat $TARGET_CGROUP/cgroup.controllers
cpu io memory pids
```

Optionally modify limits e.g.:
```sh
echo 0 > $TARGET_CGROUP/memory.swap.max
echo 50000000 > $TARGET_CGROUP/memory.max
```

Run `cgexec-rs` with a command that should be attached to the `TARGET_CGROUP` cgroup. This can be done from inside or outside the `systemd-run` shell.
```
$ cgexec-rs $TARGET_CGROUP cat /proc/self/cgroup
0::/user.slice/user-1000.slice/user@1000.service/my.slice/temporary
```

# Troubleshooting

## Verifying delegation and fixing cgroup.subtree_control

Start new shell in a new systemd slice:
```sh
$ systemd-run --user -p Delegate=yes --slice=temp1.slice --shell
```
Inside this shell, examine the current cgroup:
```sh
$ cat /proc/self/cgroup
0::/user.slice/user-1000.slice/user@1000.service/my.slice/run-u690.service
```
Verify that `cgroup.controller` has all required controllers:
```sh
$ cat /sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/cgroup.subtree_control
memory pids
```
If some controllers are missing, go up the hierarchy and add them to `cgroup.subtree_control`.
```sh
$ echo +cpu > /sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/cgroup.subtree_control
$ echo +io > /sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/cgroup.subtree_control
$ cat /sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/cgroup.subtree_control
cpu io memory pids
```
