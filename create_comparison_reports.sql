USE cphl_review;

DROP TABLE IF EXISTS report_monthly_packages_missing_in_vl;
DROP TABLE IF EXISTS report_monthly_packages_exists_in_vl;
DROP TABLE IF EXISTS report_monthly_vl_missing_in_packages;
DROP TABLE IF EXISTS report_monthly_vl_exists_in_packages;
DROP TABLE IF EXISTS report_packages_missing_in_vl;
DROP TABLE IF EXISTS report_packages_exists_in_vl;
DROP TABLE IF EXISTS report_vl_missing_in_packages;
DROP TABLE IF EXISTS report_vl_exists_in_packages;
DROP TABLE IF EXISTS report_monthly_comparison;
DROP TABLE IF EXISTS report_overall_comparison;
DROP TABLE IF EXISTS vl_monthly_unique_tracking_codes;

CREATE TABLE vl_monthly_unique_tracking_codes AS
SELECT *
FROM vl_data
WHERE 1 = 0;

ALTER TABLE vl_monthly_unique_tracking_codes
  MODIFY `source_file` VARCHAR(32) NOT NULL,
  MODIFY `tracking_code` VARCHAR(255) NOT NULL;

INSERT INTO vl_monthly_unique_tracking_codes
SELECT
  `source_file`,
  `form_number`,
  `facility_reference`,
  TRIM(REPLACE(REPLACE(REPLACE(REPLACE(`tracking_code`, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', '')) AS tracking_code,
  `facility`,
  `district`,
  `region`,
  `hub`,
  `date_collected`,
  `date_received`,
  `date_created`,
  `data_entered_at`,
  `sample_type`,
  `s.barcode`,
  `s.barcode2`,
  `s.barcode3`,
  `art_number`,
  `other_id`,
  `unique_id`,
  `sex`,
  `date_of_birth`,
  `age`,
  `treatment_initiation_date`,
  `treatment_duration`,
  `current_regimen`,
  `other_regimen`,
  `indication_for_VL_Testing`,
  `failure_reason`,
  `pregnant`,
  `anc_number`,
  `breast_feeding`,
  `active_tb_status`,
  `tb_treatment_phase`,
  `arv_adherence`,
  `status`,
  `approval_date`,
  `rejection_reason_id`,
  `rejection_reason`,
  `treatment_line`,
  `treatment_line_id`,
  `result_alphanumeric`,
  `suppressed`,
  `result_upload_date`,
  `released_at`,
  `current_who_stage`,
  `dhis2_name`,
  `dhis2_uid`,
  `test_date`,
  `data_qc_date_for_rejects`,
  `date_downloaded`,
  `brod_consent`,
  `test_machine`,
  `current_regimen_initiation_date`,
  `delivered_at`,
  `picked_from_facility_on`,
  `is_reviewed_for_dr`,
  `data_entered_by_id`,
  `hie_data_created_at`,
  `source_system`
FROM (
  SELECT
    vl_data.*,
    ROW_NUMBER() OVER (
      PARTITION BY
        `source_file`,
        TRIM(REPLACE(REPLACE(REPLACE(REPLACE(`tracking_code`, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))
      ORDER BY
        NULLIF(`date_created`, '') ASC,
        NULLIF(`date_received`, '') ASC,
        NULLIF(`date_collected`, '') ASC,
        `form_number` ASC
    ) AS row_rank
  FROM vl_data
  WHERE LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(`tracking_code`, ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))) NOT IN ('', 'null', 'none', 'non')
) ranked_rows
WHERE row_rank = 1
ORDER BY source_file, tracking_code;

ALTER TABLE vl_monthly_unique_tracking_codes
  ADD PRIMARY KEY (`source_file`, `tracking_code`);

CREATE TABLE report_overall_comparison AS
SELECT
  (SELECT COUNT(*) FROM vl_data_unique_tracking_codes) AS total_unique_vl_tracking_codes,
  (SELECT COUNT(*) FROM packages_unique_barcodes) AS total_unique_package_barcodes,
  (SELECT COUNT(*)
   FROM vl_data_unique_tracking_codes v
   INNER JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code`) AS exists_in_both,
  (SELECT COUNT(*)
   FROM vl_data_unique_tracking_codes v
   LEFT JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code`
   WHERE p.`barcode` IS NULL) AS vl_tracking_missing_in_packages,
  (SELECT COUNT(*)
   FROM packages_unique_barcodes p
   LEFT JOIN vl_data_unique_tracking_codes v ON v.`tracking_code` = p.`barcode`
   WHERE v.`tracking_code` IS NULL) AS package_barcodes_missing_in_vl,
  ROUND(
    (SELECT COUNT(*)
     FROM vl_data_unique_tracking_codes v
     INNER JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code`)
    / (SELECT COUNT(*) FROM vl_data_unique_tracking_codes) * 100,
    2
  ) AS percent_of_vl_found_in_packages,
  ROUND(
    (SELECT COUNT(*)
     FROM packages_unique_barcodes p
     INNER JOIN vl_data_unique_tracking_codes v ON v.`tracking_code` = p.`barcode`)
    / (SELECT COUNT(*) FROM packages_unique_barcodes) * 100,
    2
  ) AS percent_of_packages_found_in_vl;

CREATE TABLE report_monthly_comparison AS
SELECT
  v.source_file,
  COUNT(*) AS unique_vl_tracking_codes,
  COUNT(p.`barcode`) AS vl_tracking_exists_in_packages,
  COUNT(*) - COUNT(p.`barcode`) AS vl_tracking_missing_in_packages,
  (SELECT COUNT(*) FROM packages_unique_barcodes) AS unique_package_barcodes,
  (
    SELECT COUNT(*)
    FROM packages_unique_barcodes p2
    INNER JOIN vl_monthly_unique_tracking_codes v2
      ON v2.`tracking_code` = p2.`barcode`
    WHERE v2.source_file = v.source_file
  ) AS package_barcodes_exists_in_vl_month,
  (
    SELECT COUNT(*)
    FROM packages_unique_barcodes p2
    LEFT JOIN vl_monthly_unique_tracking_codes v2
      ON v2.`tracking_code` = p2.`barcode`
      AND v2.source_file = v.source_file
    WHERE v2.`tracking_code` IS NULL
  ) AS package_barcodes_missing_in_vl_month,
  ROUND(COUNT(p.`barcode`) / COUNT(*) * 100, 2) AS percent_of_vl_found_in_packages,
  ROUND((
    SELECT COUNT(*)
    FROM packages_unique_barcodes p2
    INNER JOIN vl_monthly_unique_tracking_codes v2
      ON v2.`tracking_code` = p2.`barcode`
    WHERE v2.source_file = v.source_file
  ) / (SELECT COUNT(*) FROM packages_unique_barcodes) * 100, 2) AS percent_of_packages_found_in_vl_month
FROM vl_monthly_unique_tracking_codes v
LEFT JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code`
GROUP BY v.source_file
ORDER BY v.source_file;

CREATE TABLE report_vl_exists_in_packages AS
SELECT v.*
FROM vl_data_unique_tracking_codes v
INNER JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code`
ORDER BY v.`tracking_code`;

CREATE TABLE report_vl_missing_in_packages AS
SELECT v.*
FROM vl_data_unique_tracking_codes v
LEFT JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code`
WHERE p.`barcode` IS NULL
ORDER BY v.`tracking_code`;

CREATE TABLE report_packages_exists_in_vl AS
SELECT p.*
FROM packages_unique_barcodes p
INNER JOIN vl_data_unique_tracking_codes v ON v.`tracking_code` = p.`barcode`
ORDER BY p.`barcode`;

CREATE TABLE report_packages_missing_in_vl AS
SELECT p.*
FROM packages_unique_barcodes p
LEFT JOIN vl_data_unique_tracking_codes v ON v.`tracking_code` = p.`barcode`
WHERE v.`tracking_code` IS NULL
ORDER BY p.`barcode`;

CREATE TABLE report_monthly_vl_exists_in_packages AS
SELECT v.*
FROM vl_monthly_unique_tracking_codes v
INNER JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code`
ORDER BY v.source_file, v.`tracking_code`;

CREATE TABLE report_monthly_vl_missing_in_packages AS
SELECT v.*
FROM vl_monthly_unique_tracking_codes v
LEFT JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code`
WHERE p.`barcode` IS NULL
ORDER BY v.source_file, v.`tracking_code`;

CREATE TABLE report_monthly_packages_exists_in_vl AS
SELECT v.source_file, p.*
FROM packages_unique_barcodes p
INNER JOIN vl_monthly_unique_tracking_codes v ON v.`tracking_code` = p.`barcode`
ORDER BY v.source_file, p.`barcode`;

CREATE TABLE report_monthly_packages_missing_in_vl AS
SELECT months.source_file, p.*
FROM (SELECT DISTINCT source_file FROM vl_monthly_unique_tracking_codes) months
CROSS JOIN packages_unique_barcodes p
LEFT JOIN vl_monthly_unique_tracking_codes v
  ON v.source_file = months.source_file
  AND v.`tracking_code` = p.`barcode`
WHERE v.`tracking_code` IS NULL
ORDER BY months.source_file, p.`barcode`;

ALTER TABLE report_vl_exists_in_packages ADD PRIMARY KEY (`tracking_code`);
ALTER TABLE report_vl_missing_in_packages ADD PRIMARY KEY (`tracking_code`);
ALTER TABLE report_packages_exists_in_vl ADD PRIMARY KEY (`barcode`);
ALTER TABLE report_packages_missing_in_vl ADD PRIMARY KEY (`barcode`);
ALTER TABLE report_monthly_vl_exists_in_packages ADD PRIMARY KEY (`source_file`, `tracking_code`);
ALTER TABLE report_monthly_vl_missing_in_packages ADD PRIMARY KEY (`source_file`, `tracking_code`);
ALTER TABLE report_monthly_packages_exists_in_vl ADD PRIMARY KEY (`source_file`, `barcode`);
ALTER TABLE report_monthly_packages_missing_in_vl ADD PRIMARY KEY (`source_file`, `barcode`);
