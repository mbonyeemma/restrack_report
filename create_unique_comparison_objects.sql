USE cphl_review;

DROP TABLE IF EXISTS vl_data_unique_tracking_codes;
DROP TABLE IF EXISTS packages_unique_barcodes;

CREATE TABLE vl_data_unique_tracking_codes AS
SELECT *
FROM vl_data
WHERE 1 = 0;

ALTER TABLE vl_data_unique_tracking_codes
  MODIFY `tracking_code` VARCHAR(255) NOT NULL;

INSERT INTO vl_data_unique_tracking_codes
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
      PARTITION BY TRIM(REPLACE(REPLACE(REPLACE(REPLACE(`tracking_code`, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))
      ORDER BY
        NULLIF(`date_created`, '') ASC,
        NULLIF(`date_received`, '') ASC,
        NULLIF(`date_collected`, '') ASC,
        `source_file` ASC,
        `form_number` ASC
    ) AS row_rank
  FROM vl_data
  WHERE LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(`tracking_code`, ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))) NOT IN ('', 'null', 'none', 'non')
) ranked_rows
WHERE row_rank = 1
ORDER BY tracking_code;

ALTER TABLE vl_data_unique_tracking_codes
  ADD PRIMARY KEY (`tracking_code`);

CREATE TABLE packages_unique_barcodes AS
SELECT *
FROM packages
WHERE 1 = 0;

ALTER TABLE packages_unique_barcodes
  MODIFY `barcode` VARCHAR(255) NOT NULL;

INSERT INTO packages_unique_barcodes
SELECT
  `id`,
  `parent_id`,
  TRIM(REPLACE(REPLACE(REPLACE(REPLACE(`barcode`, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', '')) AS barcode,
  `barcode_id`,
  `facilityid`,
  `case_id`,
  `hubid`,
  `latest_event_id`,
  `test_type`,
  `sample_type`,
  `type`,
  `status`,
  `is_merged`,
  `is_batch`,
  `first_received_at`,
  `place_name`,
  `latitude`,
  `longitude`,
  `numberofsamples`,
  `numberofpackages`,
  `numberofsamplesreceived`,
  `current_holder`,
  `delivered_on`,
  `delivered_by`,
  `received_at_destination_on`,
  `received_by`,
  `final_destination`,
  `is_tracked_from_facility`,
  `date_picked`,
  `created_by`,
  `created_at`,
  `updated_at`,
  `collected_at`
FROM (
  SELECT
    packages.*,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(REPLACE(REPLACE(REPLACE(REPLACE(`barcode`, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))
      ORDER BY
        NULLIF(`created_at`, '') ASC,
        NULLIF(`updated_at`, '') ASC,
        NULLIF(`date_picked`, '') ASC,
        `id` ASC
    ) AS row_rank
  FROM packages
  WHERE LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(`barcode`, ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))) NOT IN ('', 'null', 'none', 'non')
) ranked_rows
WHERE row_rank = 1
ORDER BY barcode;

ALTER TABLE packages_unique_barcodes
  ADD PRIMARY KEY (`barcode`);
