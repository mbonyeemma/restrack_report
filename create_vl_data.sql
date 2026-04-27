USE cphl_review;

DROP TABLE IF EXISTS vl_data;

CREATE TABLE vl_data LIKE results_202602;

ALTER TABLE vl_data
  ADD COLUMN `source_file` VARCHAR(32) NOT NULL FIRST;

INSERT INTO vl_data
SELECT '202602.csv' AS source_file, results_202602.* FROM results_202602;

INSERT INTO vl_data
SELECT '202603.csv' AS source_file, results_202603.* FROM results_202603;

INSERT INTO vl_data
SELECT '202604.csv' AS source_file, results_202604.* FROM results_202604;

CREATE INDEX vl_data_tracking_code_idx ON vl_data (`tracking_code`(191));
CREATE INDEX vl_data_source_file_idx ON vl_data (`source_file`);
