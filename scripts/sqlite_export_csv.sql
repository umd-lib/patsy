.headers on
.mode csv

.output filename_only_matches.csv
SELECT
  filename_only_matches.*
FROM
  filename_only_matches,
  accessions
WHERE
  filename_only_matches.accession_id = accessions.id
;

.output perfect_matches.csv
SELECT
  perfect_matches.*
FROM
  perfect_matches,
  accessions
WHERE
  perfect_matches.accession_id = accessions.id
;

.output altered_md5_matches.csv
SELECT
  altered_md5_matches.*
FROM
  altered_md5_matches,
  accessions
WHERE
  altered_md5_matches.accession_id = accessions.id
;

.output transfers.csv
SELECT
  *
FROM
  transfers
;

.output accessions.csv
SELECT
  *
FROM
  accessions
;

.output restores.csv
SELECT
  *
FROM
  restores
;

.quit