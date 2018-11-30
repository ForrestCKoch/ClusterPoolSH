#!/bin/bash

###############################################################################
# ClusterPool.sh
# TODO: Insert description & author details
###############################################################################

###############################################################################
# Helper Functions
###############################################################################
usage(){
    echo "Usage: $0 [Options] 
    
    Options:
    -h|--help                   Show this help message
    -i|--init                   Initialize working directory 
    -w|--workdir [PATH]         Set working directory path (Default: pwd)
    -b|--batches [N]            Set number of batches (Default: 1)
    -p|--init-pool [NAME]       Initialize new pool with name [NAME]
    -n|--max-workers [N]        Set poolsize (Default: 4)
    -t|--max-time [N]           Don't schedule jobs exceeding this time 
    -a|--add \"COMMAND\" [NAME]   Add command to queue *NEEDS \"s
    -B|--set-batch              Set the batch for command (Default: 1) 
    -P|--priority [1-$NPRIORS]         Set priority of job (Default 4)
    -d|--degrade                Degrade priority after reschedule
">&2
    #-r|--reschedule [y/n]       Whether to reschedule jobs (Default: y)     
}

# check whether $WORKING_DIR is valid
check_dir(){
    if [ ! -d "$WORKING_DIR/queued" ]; then
        echo "Error: $WORKING_DIR is an invalid directory">&2
        exit 1
    elif [ ! -d "$WORKING_DIR/locks" ]; then
        echo "Error: $WORKING_DIR is an invalid directory">&2
        exit 1
    elif [ ! -d "$WORKING_DIR/failed" ]; then
        echo "Error: $WORKING_DIR is an invalid directory">&2
        exit 1
    elif [ ! -d "$WORKING_DIR/pools" ]; then
        echo "Error: $WORKING_DIR is an invalid directory">&2
        exit 1
    elif [ ! -d "$WORKING_DIR/complete" ]; then
        echo "Error: $WORKING_DIR is an invalid directory">&2
        exit 1
    elif [ ! -d "$WORKING_DIR/logs" ]; then
        echo "Error: $WORKING_DIR is an invalid directory">&2
        exit 1
    fi
    for i in $(seq 1 $NPRIORS); do
        for j in $(seq 1 $BATCHES); do
            if [ ! -d "$WORKING_DIR/queued/$i/$j" ]; then
                echo "Error: $WORKING_DIR is an invalid directory">&2
                exit 1
            fi
        done
    done
    for i in $(seq 1 $BATCHES); do
        if [ ! -d "$WORKING_DIR/complete/$i" ]; then
            echo "Error: $WORKING_DIR is an invalid directory">&2
            exit 1
        fi    
    done
    for i in $(seq 1 $BATCHES); do
        if [ ! -d "$WORKING_DIR/failed/$i" ]; then
            echo "Error: $WORKING_DIR is an invalid directory">&2
            exit 1
        fi    
    done
}

# setup $WORKING_DIR to be a valid working directory
setup_directory(){
    mkdir -p "$WORKING_DIR/queued"
    # Attempt to remove old folders
    rmdir $WORKDING_DIR/queued/*/* 2>/dev/null
    rmdir $WORKDING_DIR/queued/* 2>/dev/null
    for i in $(seq 1 $NPRIORS); do 
        for j in $(seq 1 $BATCHES); do
            mkdir -p "$WORKING_DIR/queued/$i/$j"; 
        done
    done
    mkdir -p "$WORKING_DIR/failed"
    for i in $(seq 1 $BATCHES); do
        mkdir -p "$WORKING_DIR/failed/$i" 
    done
    mkdir -p "$WORKING_DIR/locks"
    mkdir -p "$WORKING_DIR/pools"
    mkdir -p "$WORKING_DIR/complete"
    for i in $(seq 1 $BATCHES); do
        mkdir -p "$WORKING_DIR/complete/$i" 
    done
    mkdir -p "$WORKING_DIR/logs"
}

# setup a pool
setup_localpool(){
    if  mkdir "$WORKING_DIR/pools/$1" ; then
        #LOCAL_POOL=$1
        return
    else
        echo "Error: Couldn't create pool $1" >&2
        exit 1
    fi
}

# tear down our pool
destroy_localpool(){
    echo 'hi'
    for job in $(ls $WORKING_DIR/pools/$LOCAL_POOL/*.job); do
        fname=$(echo $job|rev|cut -d'/' -f1|cut -d'.' -f2-|rev)
        kill_job $fname
    done
    rmdir "$WORKING_DIR/pools/$LOCAL_POOL"
}

get_poolsize(){
    ls "$WORKING_DIR/pools/$LOCAL_POOL"|wc -l
}

# add command to queue
# NOTE: It is the user's responsiblity to make sure each job
#       is given a unique name.  Behaviour is otherwise undefined
# $1 -- command
# $2 -- job name
add_command(){
    local name="$2"
    local command_string="$1"
    echo "$command_string" > "$WORKING_DIR/queued/$PRIORITY/$BAT/$name.job"
}

# return the next job to be run
next_job(){
    for p in $(seq 1 $NPRIORS); do
        for b in $(seq 1 $BATCHES); do
            local head_job=$(ls $WORKING_DIR/queued/$p/$b/* 2>/dev/null|head -n1)
            if [ $head_job ]; then
                echo $head_job|rev|cut -d'/' -f1-3|rev
                return
            fi
        done
    done
}

# attempt to lock the job --
# remeber to check the return status!
lock_job(){
    echo locking echo $1
    mkdir "$WORKING_DIR/locks/$1.lock" 2>/dev/null
}

unlock_job(){
    rmdir "$WORKING_DIR/locks/$1.lock"
}

# Move job from queue into pool and run
# This function should only be called as a forked process
# $1 -- job name
# $2 -- batch
# $3 -- priority
run_job(){
    local queuefile="$WORKING_DIR/queued/$3/$2/$1.job"
    if [ $BASHPID = $$ ]; then
        echo "Error: Parent processes can't run jobs!" >&2
    elif [ ! -f $queuefile ]; then
        echo "Error: $queuefile not found!" >&2
    else
        #trap _term_worker SIGTERM SIGINT
        trap _term SIGTERM SIGINT
        local runfile="$WORKING_DIR/pools/$LOCAL_POOL/$3-$2-$BASHPID-$1.job"
        local logfile="$WORKING_DIR/logs/$1.log"
        local endfile="$WORKING_DIR/complete/$2/$1.job"
        mv "$queuefile" "$runfile"

        # Start the job as a forked process, and record
        # the process in case we recieve a SIGTERM
        START=$(( $(date +%s)/60 ))
        bash "$runfile" >> "$logfile" &
        JPID="$!"
        wait
        mv "$runfile" "$endfile"
    fi
}

# Kill the indicated job and return it to the queue
# Note, it is up to the job to capture SIGTERM
kill_job(){
    local jname=$(echo $1|cut -d'-' -f4-)
    local batch=$(echo $1|cut -d'-' -f2)
    local prior=$(echo $1|cut -d'-' -f1)

    # degrade if necessary
    if [ $DEGRADE ]; then
        prior=$(( prior + 1 ))
    fi

    local queuefile="$WORKING_DIR/queued/$prior/$batch/$jname.job"
    local failed="$WORKING_DIR/failed/$batch/$jname.job"
    local runfile="$1.job"
    local logfile="$WORKING_DIR/logs/$jname.log"
    local proc_id=$(echo $1|cut -d'-' -f3)
    echo killing $1 on $proc_id
    kill $proc_id 2>/dev/null

    # Don't move back if we've exceeded our runtime
    # Or if we've exceeded priorities/attempts
    STOP=$(( $(date +%s)/60 ))
    if [ $MAX_TIME ] && [ $(( START - STOP )) -gt $MAX_TIME ]; then
        mv "$WORKING_DIR/pools/$LOCAL_POOL/$runfile" "$failed"
    elif [ $prior -gt $NPRIORS ]; then
        mv "$WORKING_DIR/pools/$LOCAL_POOL/$runfile" "$failed"
    else 
        mv "$WORKING_DIR/pools/$LOCAL_POOL/$runfile" "$queuefile"
        rm $logfile
    fi
    unlock_job $jname
}

###############################################################################
# SIGTERM/SIGINT cleanup
###############################################################################
_term(){
    if [ $BASHPID = $$ ]; then 
        #echo "Hello from termpool $BASHPID"
        destroy_localpool
    else
        #echo "Hello from termworker $BASHPID, now killing $JPID"
        kill $JPID 2>/dev/null
    fi
    echo "goodbye"
    exit 1
}

trap _term SIGTERM SIGINT

###############################################################################
# Argument Variable Declaration
###############################################################################
WORKING_DIR="$(pwd)"
NPRIORS=9
BATCHES=1
BAT=1
INIT_DIR=''
LOCAL_POOL=''
MAX_WORKERS=4
MAX_TIME='' # minutes
PRIORITY=4
COMMAND=''
DEGRADE=''

###############################################################################
# Argument Parsing
###############################################################################
while [[ $# -gt 0 ]];
do
key="$1"
case $key in

    -h|--help)
        usage
        exit 0
    ;;
    -i|--init)
        INIT_DIR=0 
        shift
    ;;
    -w|--working-directory)
        WORKING_DIR="$2"
        shift
        shift
    ;;
    -b|--batches)
        BATCHES="$2"
        shift
        shift
    ;;
    -B|--set-batch)
        BAT="$2"
        shift
        shift
    ;;
    -p|--init-pool)
        LOCAL_POOL="$2"
        shift
        shift
    ;;
    -n|--max-workers)
        MAX_WORKERS="$2"
        shift
        shift
    ;;
    -t|--max-time)
        MAX_TIME="$2"
        shift
        shift
    ;;
    -P|--priority)
        PRIORITY="$2"
        shift
        shift
    ;;
    -a|--add)
        COMMAND="$2"
        JNAME="$3"
        shift
        shift
        shift
    ;;
    -d|--degrade)
        DEGRADE=0
        shift
    ;; 
esac
done

###############################################################################
# Argument & Directory Checking
###############################################################################
if [ $INIT_DIR ]; then
    setup_directory
    check_dir
else # we need to get some information about batches/priorities
    NPRIORS=$(ls $WORKING_DIR/queued|wc -l) 
    BATCHES=$(ls $WORKING_DIR/queued/1|wc -l)
    check_dir
fi

echo $COMMAND
if [ "$COMMAND" ]; then
    add_command "$COMMAND" "$JNAME"
fi

if [ $LOCAL_POOL ]; then
    setup_localpool "$LOCAL_POOL"

    next=$(next_job)
    jname=$(echo $next|cut -d'/' -f3|rev|cut -d'.' -f2-|rev)
    batch=$(echo $next|cut -d'/' -f2)
    prior=$(echo $next|cut -d'/' -f1)
    while [ $next ]; do
        if [ $(get_poolsize) -lt 4 ]; then
            lock_job $jname
            if [ $? = 0 ]; then
                echo "Starting job $next"
                run_job $jname $batch $prior & disown
            else
                waittime=$(( (RANDOM % 5) ))
                echo "Couldn't lock $next, waiting ${waittime}s..."
                sleep $waittime
            fi
        else
            echo "Pool full waiting 5s..." 
            sleep 5
        fi
        next=$(next_job)
        jname=$(echo $next|cut -d'/' -f3|rev|cut -d'.' -f2-|rev)
        batch=$(echo $next|cut -d'/' -f2)
        prior=$(echo $next|cut -d'/' -f1)
    done

    echo 'hi'
    destroy_localpool
fi

