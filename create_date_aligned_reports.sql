USE cphl_review;

DROP TABLE IF EXISTS report_same_range_overall_comparison;
DROP TABLE IF EXISTS report_same_range_monthly_comparison;

CREATE TABLE report_same_range_overall_comparison AS
WITH vl_range AS (
  SELECT
    MIN(LEFT(TRIM(date_created), 10)) AS start_date,
    MAX(LEFT(TRIM(date_created), 10)) AS end_date
  FROM vl_data
  WHERE LEFT(TRIM(date_created), 10) BETWEEN '2026-01-01' AND '2026-12-31'
),
period_vl AS (
  SELECT DISTINCT
    TRIM(REPLACE(REPLACE(REPLACE(REPLACE(tracking_code, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', '')) AS tracking_code
  FROM vl_data, vl_range
  WHERE LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(tracking_code, ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))) NOT IN ('', 'null', 'none', 'non')
    AND LEFT(TRIM(date_created), 10) BETWEEN vl_range.start_date AND vl_range.end_date
),
period_packages AS (
  SELECT DISTINCT
    TRIM(REPLACE(REPLACE(REPLACE(REPLACE(barcode, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', '')) AS barcode
  FROM packages, vl_range
  WHERE LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(barcode, ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))) NOT IN ('', 'null', 'none', 'non')
    AND LEFT(TRIM(created_at), 10) BETWEEN vl_range.start_date AND vl_range.end_date
)
SELECT
  (SELECT start_date FROM vl_range) AS start_date,
  (SELECT end_date FROM vl_range) AS end_date,
  (SELECT COUNT(*) FROM period_vl) AS total_unique_vl_tracking_codes,
  (SELECT COUNT(*) FROM period_packages) AS total_unique_package_barcodes,
  (SELECT COUNT(*) FROM period_vl v INNER JOIN period_packages p ON p.barcode = v.tracking_code) AS exists_in_both,
  (SELECT COUNT(*) FROM period_vl v LEFT JOIN period_packages p ON p.barcode = v.tracking_code WHERE p.barcode IS NULL) AS vl_tracking_missing_in_packages,
  (SELECT COUNT(*) FROM period_packages p LEFT JOIN period_vl v ON v.tracking_code = p.barcode WHERE v.tracking_code IS NULL) AS package_barcodes_missing_in_vl,
  ROUND((SELECT COUNT(*) FROM period_vl v INNER JOIN period_packages p ON p.barcode = v.tracking_code) / (SELECT COUNT(*) FROM period_vl) * 100, 2) AS percent_of_vl_found_in_packages,
  ROUND((SELECT COUNT(*) FROM period_packages p INNER JOIN period_vl v ON v.tracking_code = p.barcode) / (SELECT COUNT(*) FROM period_packages) * 100, 2) AS percent_of_packages_found_in_vl;

CREATE TABLE report_same_range_monthly_comparison AS
WITH month_ranges AS (
  SELECT
    source_file,
    MIN(LEFT(TRIM(date_created), 10)) AS start_date,
    MAX(LEFT(TRIM(date_created), 10)) AS end_date
  FROM vl_data
  WHERE LEFT(TRIM(date_created), 10) BETWEEN '2026-01-01' AND '2026-12-31'
  GROUP BY source_file
),
monthly_vl AS (
  SELECT DISTINCT
    v.source_file,
    TRIM(REPLACE(REPLACE(REPLACE(REPLACE(v.tracking_code, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', '')) AS tracking_code
  FROM vl_data v
  INNER JOIN month_ranges m ON m.source_file = v.source_file
  WHERE LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(v.tracking_code, ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))) NOT IN ('', 'null', 'none', 'non')
    AND LEFT(TRIM(v.date_created), 10) BETWEEN m.start_date AND m.end_date
),
monthly_packages AS (
  SELECT DISTINCT
    m.source_file,
    TRIM(REPLACE(REPLACE(REPLACE(REPLACE(p.barcode, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', '')) AS barcode
  FROM packages p
  INNER JOIN month_ranges m ON LEFT(TRIM(p.created_at), 10) BETWEEN m.start_date AND m.end_date
  WHERE LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(p.barcode, ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))) NOT IN ('', 'null', 'none', 'non')
)
SELECT
  m.source_file,
  m.start_date,
  m.end_date,
  (SELECT COUNT(*) FROM monthly_vl v WHERE v.source_file = m.source_file) AS unique_vl_tracking_codes,
  (SELECT COUNT(*) FROM monthly_packages p WHERE p.source_file = m.source_file) AS unique_package_barcodes,
  (SELECT COUNT(*) FROM monthly_vl v INNER JOIN monthly_packages p ON p.source_file = v.source_file AND p.barcode = v.tracking_code WHERE v.source_file = m.source_file) AS exists_in_both,
  (SELECT COUNT(*) FROM monthly_vl v LEFT JOIN monthly_packages p ON p.source_file = v.source_file AND p.barcode = v.tracking_code WHERE v.source_file = m.source_file AND p.barcode IS NULL) AS vl_tracking_missing_in_packages,
  (SELECT COUNT(*) FROM monthly_packages p LEFT JOIN monthly_vl v ON v.source_file = p.source_file AND v.tracking_code = p.barcode WHERE p.source_file = m.source_file AND v.tracking_code IS NULL) AS package_barcodes_missing_in_vl,
  ROUND((SELECT COUNT(*) FROM monthly_vl v INNER JOIN monthly_packages p ON p.source_file = v.source_file AND p.barcode = v.tracking_code WHERE v.source_file = m.source_file) / (SELECT COUNT(*) FROM monthly_vl v WHERE v.source_file = m.source_file) * 100, 2) AS percent_of_vl_found_in_packages,
  ROUND((SELECT COUNT(*) FROM monthly_packages p INNER JOIN monthly_vl v ON v.source_file = p.source_file AND v.tracking_code = p.barcode WHERE p.source_file = m.source_file) / (SELECT COUNT(*) FROM monthly_packages p WHERE p.source_file = m.source_file) * 100, 2) AS percent_of_packages_found_in_vl
FROM month_ranges m
ORDER BY m.source_file;
