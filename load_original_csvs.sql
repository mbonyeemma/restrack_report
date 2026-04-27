SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

USE cphl_review;

DROP VIEW IF EXISTS monthly_results_all;
DROP VIEW IF EXISTS comparison_summary;
DROP VIEW IF EXISTS missing_restrack_barcodes;
DROP VIEW IF EXISTS found_restrack_barcodes;
DROP VIEW IF EXISTS unique_tracking_codes;
DROP VIEW IF EXISTS unique_package_barcodes;
DROP TABLE IF EXISTS results_202602;
DROP TABLE IF EXISTS results_202603;
DROP TABLE IF EXISTS results_202604;
DROP TABLE IF EXISTS packages;

CREATE TABLE packages (
  `id` TEXT,
  `parent_id` TEXT,
  `barcode` TEXT,
  `barcode_id` TEXT,
  `facilityid` TEXT,
  `case_id` TEXT,
  `hubid` TEXT,
  `latest_event_id` TEXT,
  `test_type` TEXT,
  `sample_type` TEXT,
  `type` TEXT,
  `status` TEXT,
  `is_merged` TEXT,
  `is_batch` TEXT,
  `first_received_at` TEXT,
  `place_name` TEXT,
  `latitude` TEXT,
  `longitude` TEXT,
  `numberofsamples` TEXT,
  `numberofpackages` TEXT,
  `numberofsamplesreceived` TEXT,
  `current_holder` TEXT,
  `delivered_on` TEXT,
  `delivered_by` TEXT,
  `received_at_destination_on` TEXT,
  `received_by` TEXT,
  `final_destination` TEXT,
  `is_tracked_from_facility` TEXT,
  `date_picked` TEXT,
  `created_by` TEXT,
  `created_at` TEXT,
  `updated_at` TEXT,
  `collected_at` TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE results_202602 (
  `form_number` TEXT,
  `facility_reference` TEXT,
  `tracking_code` TEXT,
  `facility` TEXT,
  `district` TEXT,
  `region` TEXT,
  `hub` TEXT,
  `date_collected` TEXT,
  `date_received` TEXT,
  `date_created` TEXT,
  `data_entered_at` TEXT,
  `sample_type` TEXT,
  `s.barcode` TEXT,
  `s.barcode2` TEXT,
  `s.barcode3` TEXT,
  `art_number` TEXT,
  `other_id` TEXT,
  `unique_id` TEXT,
  `sex` TEXT,
  `date_of_birth` TEXT,
  `age` TEXT,
  `treatment_initiation_date` TEXT,
  `treatment_duration` TEXT,
  `current_regimen` TEXT,
  `other_regimen` TEXT,
  `indication_for_VL_Testing` TEXT,
  `failure_reason` TEXT,
  `pregnant` TEXT,
  `anc_number` TEXT,
  `breast_feeding` TEXT,
  `active_tb_status` TEXT,
  `tb_treatment_phase` TEXT,
  `arv_adherence` TEXT,
  `status` TEXT,
  `approval_date` TEXT,
  `rejection_reason_id` TEXT,
  `rejection_reason` TEXT,
  `treatment_line` TEXT,
  `treatment_line_id` TEXT,
  `result_alphanumeric` TEXT,
  `suppressed` TEXT,
  `result_upload_date` TEXT,
  `released_at` TEXT,
  `current_who_stage` TEXT,
  `dhis2_name` TEXT,
  `dhis2_uid` TEXT,
  `test_date` TEXT,
  `data_qc_date_for_rejects` TEXT,
  `date_downloaded` TEXT,
  `brod_consent` TEXT,
  `test_machine` TEXT,
  `current_regimen_initiation_date` TEXT,
  `delivered_at` TEXT,
  `picked_from_facility_on` TEXT,
  `is_reviewed_for_dr` TEXT,
  `data_entered_by_id` TEXT,
  `hie_data_created_at` TEXT,
  `source_system` TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE results_202603 LIKE results_202602;
CREATE TABLE results_202604 LIKE results_202602;

LOAD DATA LOCAL INFILE '/Users/mac/work/playground/restrackreport/packages.csv'
INTO TABLE packages
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/mac/work/playground/restrackreport/202602.csv'
INTO TABLE results_202602
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/mac/work/playground/restrackreport/202603.csv'
INTO TABLE results_202603
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/mac/work/playground/restrackreport/202604.csv'
INTO TABLE results_202604
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

CREATE VIEW monthly_results_all AS
SELECT '202602.csv' AS source_file, results_202602.* FROM results_202602
UNION ALL
SELECT '202603.csv' AS source_file, results_202603.* FROM results_202603
UNION ALL
SELECT '202604.csv' AS source_file, results_202604.* FROM results_202604;

CREATE VIEW unique_package_barcodes AS
SELECT DISTINCT TRIM(`barcode`) AS barcode
FROM packages
WHERE LOWER(TRIM(`barcode`)) NOT IN ('', 'none', 'null');

CREATE VIEW unique_tracking_codes AS
SELECT DISTINCT TRIM(`tracking_code`) AS tracking_code
FROM monthly_results_all
WHERE LOWER(TRIM(`tracking_code`)) NOT IN ('', 'none', 'null');

CREATE VIEW found_restrack_barcodes AS
SELECT p.barcode
FROM unique_package_barcodes p
INNER JOIN unique_tracking_codes t ON t.tracking_code = p.barcode;

CREATE VIEW missing_restrack_barcodes AS
SELECT p.barcode
FROM unique_package_barcodes p
LEFT JOIN unique_tracking_codes t ON t.tracking_code = p.barcode
WHERE t.tracking_code IS NULL;

CREATE VIEW comparison_summary AS
SELECT
  (SELECT COUNT(*) FROM unique_package_barcodes) AS total_restrack_barcodes,
  (SELECT COUNT(*) FROM unique_tracking_codes) AS total_unique_tracking_codes,
  (SELECT COUNT(*) FROM found_restrack_barcodes) AS found_in_unique_tracking_codes,
  (SELECT COUNT(*) FROM missing_restrack_barcodes) AS missing_from_unique_tracking_codes,
  ROUND((SELECT COUNT(*) FROM found_restrack_barcodes) / (SELECT COUNT(*) FROM unique_package_barcodes) * 100, 2) AS coverage_percent;

SET FOREIGN_KEY_CHECKS = 1;
