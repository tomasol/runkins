## HA:
* split to coordinator and workers
* upgrading story?

## TODO
NO * add logging/tracing
DD * forced killing (`kill -9`)
* streaming - request should contain option to buffer
* tailing - request last n lines
NO * Health Checking
* Report resource metrics
NO: limits, authorization etc.

## Questions

### State
* Should it be stateless? State would be lost after restart
* Persistent storage to rembember job metadata - path, status etc.
* Should exitted child process be kept around until State/Stop RPC is called (once)?

### Security
* Use a simple authorization scheme. DoS prevention - limits for RPCs per user

### Streaming
* Should stdout,err automatically be buffered or waited until stream RPC is called?
* If waited, what should Status RPC do?
* Back pressure? slow client will exhaust memory

### Other
* Should Start RPC return just PID or some unique ID? (PIDs can be reused), no storage
* What if the process creates child processes?
* split to coordinator and workers?
* Authorization?
* Concurrency - two Status RPCs streaming stdout?
* Manage only child processes? Allow killing other processes?
* What about zombies - call wait on status,stop or ignore sigchld
* LOW: Start RPC - env vars, cur.dir etc
* No graceful shutdown

----

cgroups

Building
Running
Minimum Supported Rust Version: 1.54.0
