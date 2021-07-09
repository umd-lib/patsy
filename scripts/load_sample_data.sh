#!/bin/bash -x

if [ "$1" == "sqlite" ]; then
   DATABASE="sample_data.sqlite"

elif [ "$1" == "postgres" ]; then
   DATABASE="postgresql://patsy@localhost/patsy"

else
   echo Please specify 'sqlite' or 'postgres' as the database
   exit 1
fi

python3 -m patsy -d $DATABASE schema

for file in sample_data/accessions/*.csv; do
   python3 -m patsy -d $DATABASE accessions -s $file
done

for file in sample_data/restores/*.csv; do
   python3 -m patsy -d $DATABASE restores -s $file
done

for file in sample_data/transfers/*.csv; do
   python3 -m patsy -d $DATABASE transfers -s $file
done

python3 -m patsy -d $DATABASE find_perfect_matches
python3 -m patsy -d $DATABASE find_altered_md5_matches
python3 -m patsy -d $DATABASE find_filename_only_matches
python3 -m patsy -d $DATABASE find_transfer_matches

python3 -m patsy -d $DATABASE batch_stats