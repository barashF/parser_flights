#!/bin/sh

set -e

host="$1"
shift
user="$1"
shift
password="$1"
shift
dbname="$1"
shift

until PGPASSWORD="$password" psql -h "$host" -U "$user" -d "$dbname" -c '\q'; do
  >&2 echo "PostgreSQL is unavailable - sleeping"
  sleep 1
done

>&2 echo "PostgreSQL is up - executing command"
exec "$@"