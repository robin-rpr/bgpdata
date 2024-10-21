#!/bin/bash
host=$1
shift
cmd="$@"

until nc -z "$host" 22; do
  >&2 echo "Service $host is unavailable - sleeping"
  sleep 4
done

>&2 echo "Service $host is up - executing command"
exec $cmd
