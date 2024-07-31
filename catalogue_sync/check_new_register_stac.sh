#!/bin/bash
# This script is meant for being called regularly by cron

mkdir -p /var/tmp/sentinel/

SCRIPTNAME="`basename -s .sh $0`"
LOCK="/var/tmp/sentinel/$SCRIPTNAME.lock"
VARPREFIX="/var/tmp/sentinel/get_new_list_processed.txt"

if [ -e $LOCK ]; then
	1>&2 printf "Exiting: Lock file exists: $LOCK\n\"$SCRIPTNAME\" is only meant to be run once at a time.\n\n"
	exit 1
else
	touch $LOCK
	trap "rm \"$LOCK\"" EXIT
fi

python3 get_new_list.py > "$VARPREFIX"

cat "$VARPREFIX" | while read id; do
	python3 register_stac.py -p -i $id
done
