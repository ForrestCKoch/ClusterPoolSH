#!/bin/bash
###############################################################################
# jobpool.sh
# TODO: Insert description & author details
###############################################################################

###############################################################################
# Helper Functions
###############################################################################
usage(){
    echo "Usage: $0 PoolDirectory">&2
}

# check whether $POOL_DIR is valid
check_dir(){
    if [ ! -d "$POOL_DIR/queued" ]; then
        echo "Error: $POOL_DIR is an invalid directory">&2
        exit 1
    elif [ ! -d "$POOL_DIR/locks" ]; then
        echo "Error: $POOL_DIR is an invalid directory">&2
        exit 1
    elif [ ! -d "$POOL_DIR/pools" ]; then
        echo "Error: $POOL_DIR is an invalid directory">&2
        exit 1
    elif [ ! -d "$POOL_DIR/complete" ]; then
        echo "Error: $POOL_DIR is an invalid directory">&2
        exit 1
    elif [ ! -d "$POOL_DIR/logs" ]; then
        echo "Error: $POOL_DIR is an invalid directory">&2
        exit 1
    fi
}

# setup $POOL_DIR to be a valid working directory
setup_directory(){
    mkdir -p "$POOL_DIR/queued"
    mkdir -p "$POOL_DIR/locks"
    mkdir -p "$POOL_DIR/pools"
    mkdir -p "$POOL_DIR/complete"
    mkdir -p "$POOL_DIR/logs"
}

# setup a pool
setup_localpool(){
    if  mkdir "$POOL_DIR/pools/$1" ; then
        LOCAL_POOL=$1
    else
        echo "Error: Couldn't create pool $1" >&2
    fi
}

# tear down our pool
destroy_localpool(){
    for job in "$POOL_DIR/pools/$LOCAL_POOL/*.job"; do
        jid=$(echo $job|rev|cut -d'/' -f1|cut -d'.' -f2-|rev|cut -d'-' -f2-)
        kill_job $jid
    done
    rmdir "$POOL_DIR/pools/$LOCAL_POOL"
}

get_poolsize(){
    ls "$POOL_DIR/pools/$LOCAL_POOL"|wc -l
}

# add command to queue
# NOTE: It is the user's responsiblity to make sure each job
#       is given a unique name.  Behaviour is otherwise undefined
add_command(){
    local name="$1"
    local command_string="$2"
    echo "$command_string" > "$POOL_DIR/$name.job"
}

get_njobs(){
    ls "$POOL_DIR/queued"|wc -l
}

# return the next job to be run
next_job(){
    ls "$POOL_DIR/queued"|head -n1|rev|cut -d'/' -f1|cut -d'.' -f2-|rev
}

# attempt to lock the job --
# remeber to check the return status!
lock_job(){
    mkdir "$POOL_DIR/locks/$1.lock" 2>/dev/null
}

unlock_job(){
    rmdir "$POOL_DIR/locks/$1.lock"
}

# Move job from queue into pool and run
# This function should only be called as a forked process
run_job(){
    local queuefile="$POOL_DIR/queued/$1.job"
    if [ $BASHPID = $$ ]; then
        echo "Error: Parent processes can't run jobs!" >&2
    elif [ ! -f $queuefile ]; then
        echo "Error: $queuefile not found!" >&2
    else
        trap _term_worker SIGTERM SIGINT
        IS_PARENT="" # Used to makesure SIGTERM only affects parent
        local runfile="$POOL_DIR/pools/$LOCAL_POOL/$BASHPID-$1.job"
        local logfile="$POOL_DIR/logs/$1.log"
        local endfile="$POOL_DIR/complete/$1.job"
        mv "$queuefile" "$runfile"

        # Start the job as a forked process, and record
        # the process in case we recieve a SIGTERM
        bash "$runfile" >> "$logfile" &
        JPID="$!"
        wait
        mv "$runfile" "$endfile"
    fi
}

# Kill the indicated job and return it to the queue
# Note, it is up to the job to capture SIGTERM
kill_job(){
    local queuefile="$POOL_DIR/queued/$1.job"
    local runfile="$POOL_DIR/pools/$LOCAL_POOL/$BASHPID-$1.job"
    local logfile="$POOL_DIR/logs/$1.log"
    pid=$(echo $runfile|rev|cut -d'/' -f1|rev|cut -d'-' -f1)
    echo "killing dieeee"
    kill $pid
    mv "$runfile" "$queuefile"
    rm $logfile
    unlock_job $1
}

###############################################################################
# SIGTERM/SIGINT cleanup
###############################################################################
_term_pool(){
    if [ $IS_PARENT ]; then
        # This is the pool process
        destroy_localpool
    else
        # This is a worker process
        kill $JPID
    fi
    exit 1
}

_term_worker(){
    # This is a worker process
    echo $JPID
    kill $JPID
}

trap _term_pool SIGTERM SIGINT

###############################################################################
# Argument Parsing
###############################################################################
POOL_DIR="$(pwd)"
if [[ $# -eq 1 ]]; then
    POOL_DIR="$1"
    exit 1
fi

IS_PARENT=0

###############################################################################
# Argument & Directory Checking
###############################################################################

setup_directory
check_dir
setup_localpool $$

while [ $(get_njobs) -gt 0 ]; do
    if [ $(get_poolsize) -lt 4 ]; then
        next=$(next_job)
        lock_job $next
        if [ $? = 0 ]; then
            echo "Starting job $next"
            run_job $next &
        else
            waittime=$(( (RANDOM % 5) ))
            echo "Couldn't lock $next, waiting ${waittime}s..."
            sleep $waittime
        fi
    else
        echo "Pool full waiting 5s..." 
        sleep 5
    fi
done

destroy_localpool
