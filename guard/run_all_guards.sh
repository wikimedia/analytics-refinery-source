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
  --verbose       - Increase verbosity

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
            ;;
        "--help" )
            print_help
            exit
            ;;
        "--quiet" )
            VERBOSITY=0
            ;;
        "--verbose" )
            VERBOSITY=$((VERBOSITY+1))
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
    if [ "${GUARD_FILE_ABS: -19}" != "/tools/run_guard.sh" ]
    then
        if [ -e "$GUARD_FILE_ABS" ]
        then
            if [ "$VERBOSITY" -ge 1 ]
            then
                echo "Running guard: $GUARD_FILE_ABS"
            fi
            "$GUARD_FILE_ABS"
        else
            error "Could not find a guard to run"
        fi
    fi
done