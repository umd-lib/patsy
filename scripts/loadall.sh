#!/bin/bash -x

# Simple load script for local development

cat accessions.csv | psql --host=localhost --port=5432 --user=patsy --command="copy accessions from stdin with csv header;"

cat restores.csv | psql --host=localhost --port=5432 --user=patsy --command="copy restores from stdin with csv header;"

cat transfers.csv | psql --host=localhost --port=5432 --user=patsy --command="copy transfers from stdin with csv header;"

cat filename_only_matches.csv | psql --host=localhost --port=5432 --user=patsy --command="copy filename_only_matches from stdin with csv header;"

cat altered_md5_matches.csv | psql --host=localhost --port=5432 --user=patsy --command="copy altered_md5_matches from stdin with csv header;"

cat perfect_matches.csv | psql --host=localhost --port=5432 --user=patsy --command="copy perfect_matches from stdin with csv header;"
