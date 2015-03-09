#!/bin/bash

#---------------------------------------------------------------------
source "$(dirname "$0")/tools/common.inc"
#---------------------------------------------------------------------

print_help() {
    cat <<EOF
run_all_guards.sh OPTIONS

Runs all available guards.

OPTIONS:
  --help          - prints this page
  --quiet         - Only print errors
  --rebuild-jar   - rebuilds the refinery-tools jar before running guards

EOF
}

VERBOSITY=1
REBUILD_JAR=no

# parse parameters
while [ $# -gt 0 ]
do
    ARGUMENT="$1"
    case "$ARGUMENT" in
        "--rebuild-jar" )
            REBUILD_JAR=yes
            shift
            ;;
        "--help" )
            print_help
            exit
            ;;
        "--quiet" )
            VERBOSITY=0
            ;;
        * )
            error "Unknown argument '$1'"
    esac
    shift || true
done

#Rebuild jar if requested
if [ "$REBUILD_JAR" = "yes" ]
then
    pushd .. >/dev/null
    if [ "$VERBOSITY" -ge 1 ]
    then
        mvn clean package
    else
        mvn clean package &>/dev/null
    fi
    popd >/dev/null
fi

for GUARD_FILE_ABS in "$GUARD_MAIN_DIR_ABS"/*/run_guard.sh
do
    if [ -e "$GUARD_FILE_ABS" ]
    then
        "$GUARD_FILE_ABS"
    else
        error "Could not find a guard to run"
    fi
done