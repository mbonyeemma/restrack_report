USE cphl_review;

DROP VIEW IF EXISTS comparison_summary;
DROP VIEW IF EXISTS found_restrack_barcodes;
DROP VIEW IF EXISTS missing_restrack_barcodes;
DROP VIEW IF EXISTS monthly_results_all;
DROP VIEW IF EXISTS unique_package_barcodes;
DROP VIEW IF EXISTS unique_tracking_codes;

DROP TABLE IF EXISTS results_202602;
DROP TABLE IF EXISTS results_202603;
DROP TABLE IF EXISTS results_202604;
