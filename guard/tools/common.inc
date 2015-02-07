set -e
set -o pipefail

#---------------------------------

ORIGINAL_DIR_ABS="$(pwd)"
GUARD_TOOLS_DIR_ABS="$(dirname "$BASH_SOURCE[0]")"
if [ "${GUARD_TOOLS_DIR_ABS:0:1}" != "/" ]
then
    GUARD_TOOLS_DIR_ABS="$ORIGINAL_DIR_ABS/$GUARD_TOOLS_DIR_ABS"
fi
GUARD_MAIN_DIR_ABS="$GUARD_TOOLS_DIR_ABS/.."
cd "$(dirname "$0")"
SCRIPT_DIR_ABS="$(pwd)"

#---------------------------------
error() {
    echo "Error" "$@" >&2
    exit 1
}

#---------------------------------
# Find the refinery-tools jar
set_TOOLS_JAR_FILE_ABS() {
    TOOLS_JAR_FILE_ABS="$(ls -1 --sort=time "$GUARD_MAIN_DIR_ABS"/../refinery-tools/target/refinery-tools-*.jar | tail -n 1)"

    if [ ! -e "$TOOLS_JAR_FILE_ABS" ]
    then
        error "Could not find refinery-tools jar. Please run 'mvn clean package'"
    fi
}

#---------------------------------
# Resets the arguments for the guard
reset_guard_arguments() {
    GUARD_ARGUMENTS=()
}

#---------------------------------
# Resets the arguments for the guard
add_guard_argument() {
    local ARGUMENT="$1"
    GUARD_ARGUMENTS=( "${GUARD_ARGUMENTS[@]}" "$ARGUMENT" )
}

#---------------------------------
# Default to empty parameters for guards
set_guard_arguments() {
    : # by default, no custom arguments
}
