## HA:
* split to coordinator and workers
* upgrading story?
Prior art:
* jenkins

## TODO
* add logging/tracing
* forced killing (`kill -9`)
* streaming - request should contain option to buffer
* tailing - request last n lines
* Health Checking
* Report resource metrics
* limits, authorization etc.

## Questions

### State
* Should it be stateless? If yes, should the process 'hang' as a zombie?
* Should there be a storage to remember started jobs?
* Persistent storage to rembember job metadata - path, status etc.
* Should exitted child process be kept around until State RPC is called (once)?

### Other
* Should Start RPC return just PID or some unique ID? (PIDs can be reused)
* What if the process creates child processes?
* split to coordinator and workers?
* Security, authorization?
* Manage only child processes? Allow killing other processes?
* What about zombies - call wait on status,stop or ignore sigchld
