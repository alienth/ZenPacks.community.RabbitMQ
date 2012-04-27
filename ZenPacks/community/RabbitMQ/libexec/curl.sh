#!/bin/sh

# Stupid simple wrapper to try and execute curl in the zenoss user's path (which should include the default OS paths)
# This is needed for zencommand to execute curl, since zencommand doesn't do a system PATH search

command -v curl >/dev/null 2>&1 || { echo >&2 "curl is not installed. aborting."; exit 1; }


exec curl $*
