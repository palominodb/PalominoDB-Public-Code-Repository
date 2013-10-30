#!/bin/bash
# Quick feature add for dump views definition
#
# TODO: 
#   Ideally will be nice to have this directly from pg_dump
#   Script in Python (not urgent)
#   Dump only 1 view with the "dependency" feature which is able to dump the selected view and the parents (easy fix and restore).

DB=$1
DIA=$(date +%Y%m%d%H%M)
PGBIN=/usr/local/bin
VIEWS="" ; for view in $($PGBIN/psql -U $USER $DB  -c 'select schemaname || $$.$$ ||viewname from pg_views where schemaname::text !~ $$information_schema|pg_catalog$$ ' -tA) ; do  VIEWS="$VIEWS -t $view"  ; done ; $PGBIN/pg_dump -U $USER
 -c -s $VIEWS -f view_dump.sql $DB
