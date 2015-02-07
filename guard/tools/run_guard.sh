#!/bin/bash

#---------------------------------------------------------------------
source "$(dirname "$0")/../tools/common.inc"
#---------------------------------------------------------------------

GUARD="$(basename "$(pwd)")"

source "guard_settings.inc"

set_TOOLS_JAR_FILE_ABS

reset_guard_arguments
set_guard_arguments

echo_guard_input | java -cp "$TOOLS_JAR_FILE_ABS" "org.wikimedia.analytics.refinery.tools.guard.${GUARD}Guard" "${GUARD_ARGUMENTS[@]}" "$@"
