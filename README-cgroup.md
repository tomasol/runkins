# cgroup integration

This document assumes that the cgroup v2 hierarchy is mouted at `/sys/fs/cgroup`.

### Setting up cgroup v2 with systemd-run
Systemd only allows managing following controllers by non-root users: `memory pids`.
This can be avoided by
[enabling controller delegation](https://rootlesscontaine.rs/getting-started/common/cgroup2/).

Verify that the delegation is enabled:
```sh
$ cat /sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service/cgroup.controllers
```
The needed controllers are `cpu memory io` . Follow the guide above if any of them are missing.

Start new shell in a new systemd slice:
```sh
$ systemd-run --user -p Delegate=yes --slice=my.slice --shell
```
Do not close this shell.

Inside this shell, examine the current cgroup:
```sh
$ cat /proc/self/cgroup
0::/user.slice/user-1000.slice/user@1000.service/my.slice/run-u690.service

# cd to my.slice
$ cd /sys/fs/cgroup/$(cat /proc/self/cgroup | cut -d ':' -f 3)/..
$ cat cgroup.subtree_control
cpu io memory pids
$ mkdir cgexec-rs-testing
```
Set up environment variables.
```sh
# PARENT_CGROUP will be required when running the server
export PARENT_CGROUP=/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/my.slice
export TARGET_CGROUP=$PARENT_CGROUP/cgexec-rs-testing
```

### Manually creating cgroup

To manually create a cgroup using root account:
```sh
$ export PARENT_CGROUP=/sys/fs/cgroup/my-parent
$ export TARGET_CGROUP=$PARENT_CGROUP/cgexec-rs-testing
$ mkdir $PARENT_CGROUP
```
Verify that `cgroup.subtree_control` and
`cgroup.controllers` contain all required controllers.
If some are missing, add them:
```sh
$ echo '+cpu +memory +io' > cgroup.subtree_control
$ cat cgroup.subtree_control
cpu io memory
```

Finally add a target cgroup folder.
```sh
$ mkdir $TARGET_CGROUP
```
Verify that `cgroup.controllers` is correct.

## Resources
https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html
https://systemd.io/CGROUP_DELEGATION/
https://wiki.archlinux.org/title/cgroups
https://www.redhat.com/sysadmin/cgroups-part-four
https://linux.die.net/man/1/cgexec
https://github.com/libcgroup/libcgroup
