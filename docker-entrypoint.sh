#!/bin/sh
# Bring every dataset in the catalog up to the current storage layout, then
# hand over to the container command.
#
# Migrations are idempotent and skip datasets already carrying their tag, so a
# current lake costs one catalog sweep. A failing migration aborts startup on
# purpose: the app would otherwise read a layout that isn't there.
#
# The catalog has to be reachable – on a lake root that doesn't exist yet, the
# sweep fails and so does the container.
set -e

ftm-lakehouse maintenance migrate --all

exec "$@"
