# Sample Data

## sample_data/accessions/

### Archive031.csv

Copied from "patsy-data/data/accessions/libdc/batches/sample_data/restores/LIBDCR_Archive031.csv"

File has been modified to include a "relpath" path consisting of
"DCR Projects/Archive/NOVEMBER/scpa/" plus the filename.

### test_altered_md5_accessions.csv

Accession data for testing "altered MD5" matches. There are 5 records, each
of which has an "altered md5" match with one of the restore records in
"sample_data/restores/test_altered_md5_restores.csv"

## test_filename_only_accessions.csv

Accession data for testing "filename only" matches. There are 5 records, each
of which has a "filename_only" match with one of the restore records in
"sample_data/restores/test_filename_only_restores.csv"

## alvarez.csv

The first 1,000 records from "patsy-data/data/accessions/newspapers/batches/alvarez.csv",
used for testing transfer matching.

## sample_data/restores/

### LIBDCR_Archive031.csv

Copied from "patsy-data/data/restores/LIBRDCRProjectsShare-md5sum/nfs.isip01.nas.umd.edu_ifs_data_LIBR-Archives-Projects-Export_DCR_Projects_Archive031 Date 03202014_work_scan.csv"

### test_altered_md5_restores.csv

Restore data for testing "altered MD5" matches. There are 5 records, each
of which has MD5 of the form "MD5_ALTERED_<NUMBER>" that is purposely different
from an accession record in
"sample_data/accessions/test_altered_md5_accessions.csv", but with the same
filename and file size.

### test_filename_only_restores.csv

Restore data for testing "filename only" matches. There are 5 records, each
of which has MD5 of the form "MD5_FILENAME_ONLY_<NUMBER>" that is purposely
different from an accession record in
"sample_data/accessions/test_filename_only_accessions.csv" as well as a
different file size, but with the same filename.

### alvarez.csv

The first 1,000 records from
"patsy-data/data/restores/LIBRNewsPaperShare-md5sum/nfs.isip01.nas.umd.edu_ifs_data_LIBR-Archives-Paper-Export_Historic Maryland Newspaper Project - batch_mdu_alvarez Date 9222015_work_scan.csv",
used for testing transfer matching.

## sample_data/transfers/

### alvarez.csv

A list of 1,000 records from a transfer CSV file created by the
"generate_patsy_transfer_csv.py" script using the files in the
"patsy-data/data/transfers/newspapers/alvarez/" directory.

*Note:* Only 236 transfer matches will be found when using this sample data
with the other "alvarez.csv" files.
