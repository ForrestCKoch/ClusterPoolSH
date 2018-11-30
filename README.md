# ClusterPoolSH
## Author
Forrest Koch (forrest.c.koch@gmail.com)

## Description:
This script allows the user to initialize multiple pools of workers drawing from a shared queue.  It is intended to be used alongside schedulers like SGE or PBS when the user needs to submit more jobs than the queuing system will allow.  

Pools will continue running jobs until receiving a `SIGTERM` or `SIGINT` which is normally sent after a job exceeds it's walltime.  Each running job will then be terminated and reschuled.

## Usage:
```
./cluster-pool.sh [Options]
    
    Options:
    -h|--help                   Show this help message
    
    Working Directory Options:
    -i|--init                   Initialize working directory 
    -w|--workdir [PATH]         Set working directory path (Default: pwd)
    -b|--batches [N]            Set number of batches (Default: 1)
    
    Pool Options:
    -p|--init-pool [NAME]       Initialize new pool with name [NAME]
    -n|--max-workers [N]        Set poolsize (Default: 4)
    -t|--max-time [N]           Don't schedule jobs exceeding this time (m)
    -d|--degrade                Degrade priority after reschedule
   
   
    Job Options:
    -a|--add \"COMMAND\" [NAME] Add command to queue *NEEDS \"s
    -B|--set-batch              Set the batch for command (Default: 1) 
    -P|--priority [1-9]         Set priority of job (Default 4)
```
## Example:
```bash
# Initialize current directory as a working directory
# with 8 batch folders
./cluster-pool.sh -i -b 8

# Schedule some jobs
# Note that the command is enclosed in quotes
for i in $(seq 1 1000); do
  batch=$(( i%8 + 1 ))
  ./cluster-pool.sh -a "bash script.sh arg1 arg2 ..." [Job Name] -B $batch
done

# and Start a pool with 8 workers
./cluster-pool.sh -p Pool-1 -n 8
```

## Notes on usage:
- Jobs should be divided into batches to prevent directories with too many files.
- Jobs and pool should be given unique identifiers -- behaviour is undefined otherwise.
- When adding a command to be run, remember to wrap it in quotes to pass it as a single argument.
- The degrade option will degrade the priority of a job each time it is rescheduled.  If priority is 9 before rescheduling, the job will not be rescheduled.
