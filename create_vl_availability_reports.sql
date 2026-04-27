USE cphl_review;

DROP TABLE IF EXISTS report_vl_available_list;
DROP TABLE IF EXISTS report_vl_exception_list;
DROP TABLE IF EXISTS report_vl_weekly_availability;
DROP TABLE IF EXISTS report_vl_monthly_availability;
DROP TABLE IF EXISTS report_vl_overall_availability;
DROP TABLE IF EXISTS report_original_cleaned_counts;

CREATE TABLE report_vl_available_list AS
SELECT v.*
FROM vl_data_unique_tracking_codes v
INNER JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code`
ORDER BY v.`tracking_code`;

CREATE TABLE report_vl_exception_list AS
SELECT v.*
FROM vl_data_unique_tracking_codes v
LEFT JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code`
WHERE p.`barcode` IS NULL
ORDER BY v.`tracking_code`;

ALTER TABLE report_vl_available_list ADD PRIMARY KEY (`tracking_code`);
ALTER TABLE report_vl_exception_list ADD PRIMARY KEY (`tracking_code`);

CREATE TABLE report_vl_overall_availability AS
SELECT
  COUNT(*) AS total_unique_vl_tracking_codes,
  SUM(CASE WHEN p.`barcode` IS NOT NULL THEN 1 ELSE 0 END) AS available_in_packages,
  SUM(CASE WHEN p.`barcode` IS NULL THEN 1 ELSE 0 END) AS exception_missing_in_packages,
  ROUND(SUM(CASE WHEN p.`barcode` IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS availability_percent,
  ROUND(SUM(CASE WHEN p.`barcode` IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS exception_percent,
  MIN(CASE WHEN LEFT(TRIM(v.`date_created`), 10) BETWEEN '2026-01-01' AND '2026-12-31' THEN LEFT(TRIM(v.`date_created`), 10) END) AS vl_start_date,
  MAX(CASE WHEN LEFT(TRIM(v.`date_created`), 10) BETWEEN '2026-01-01' AND '2026-12-31' THEN LEFT(TRIM(v.`date_created`), 10) END) AS vl_end_date
FROM vl_data_unique_tracking_codes v
LEFT JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code`;

CREATE TABLE report_original_cleaned_counts AS
SELECT
  'VL Data' AS dataset,
  CASE source_file
    WHEN '202602.csv' THEN 'Feb 2026'
    WHEN '202603.csv' THEN 'Mar 2026'
    WHEN '202604.csv' THEN 'Apr 2026'
    ELSE source_file
  END AS period_label,
  source_file AS period_key,
  COUNT(*) AS original_rows,
  COUNT(CASE
    WHEN LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(`tracking_code`, ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))) NOT IN ('', 'null', 'none', 'non')
    THEN 1
  END) AS rows_with_clean_key,
  COUNT(DISTINCT CASE
    WHEN LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(`tracking_code`, ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))) NOT IN ('', 'null', 'none', 'non')
    THEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(`tracking_code`, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))
  END) AS unique_clean_items
FROM vl_data
GROUP BY source_file
UNION ALL
SELECT
  'Packages' AS dataset,
  'All Package Data' AS period_label,
  'packages' AS period_key,
  (SELECT COUNT(*) FROM packages) AS original_rows,
  (SELECT COUNT(*)
   FROM packages
   WHERE LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(`barcode`, ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))) NOT IN ('', 'null', 'none', 'non')) AS rows_with_clean_key,
  (SELECT COUNT(*) FROM packages_unique_barcodes) AS unique_clean_items;

CREATE TABLE report_vl_monthly_availability AS
WITH monthly_unique_vl AS (
  SELECT
    DATE_FORMAT(STR_TO_DATE(LEFT(TRIM(`date_created`), 10), '%Y-%m-%d'), '%Y-%m') AS period_key,
    DATE_FORMAT(STR_TO_DATE(LEFT(TRIM(`date_created`), 10), '%Y-%m-%d'), '%b %Y') AS period_label,
    MIN(LEFT(TRIM(`date_created`), 10)) AS start_date,
    MAX(LEFT(TRIM(`date_created`), 10)) AS end_date,
    TRIM(REPLACE(REPLACE(REPLACE(REPLACE(`tracking_code`, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', '')) AS tracking_code
  FROM vl_data
  WHERE LEFT(TRIM(`date_created`), 10) BETWEEN '2026-01-01' AND '2026-12-31'
    AND LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(`tracking_code`, ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))) NOT IN ('', 'null', 'none', 'non')
  GROUP BY period_key, period_label, tracking_code
)
SELECT
  v.period_key,
  v.period_label,
  MIN(v.start_date) AS start_date,
  MAX(v.end_date) AS end_date,
  COUNT(*) AS unique_vl_tracking_codes,
  SUM(CASE WHEN p.`barcode` IS NOT NULL THEN 1 ELSE 0 END) AS available_in_packages,
  SUM(CASE WHEN p.`barcode` IS NULL THEN 1 ELSE 0 END) AS exception_missing_in_packages,
  ROUND(SUM(CASE WHEN p.`barcode` IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS availability_percent,
  ROUND(SUM(CASE WHEN p.`barcode` IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS exception_percent
FROM monthly_unique_vl v
LEFT JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code` COLLATE utf8mb4_unicode_ci
GROUP BY v.period_key, v.period_label
ORDER BY v.period_key;

CREATE TABLE report_vl_weekly_availability AS
WITH weekly_unique_vl AS (
  SELECT
    YEARWEEK(STR_TO_DATE(LEFT(TRIM(`date_created`), 10), '%Y-%m-%d'), 1) AS period_key,
    DATE_SUB(STR_TO_DATE(LEFT(TRIM(`date_created`), 10), '%Y-%m-%d'), INTERVAL WEEKDAY(STR_TO_DATE(LEFT(TRIM(`date_created`), 10), '%Y-%m-%d')) DAY) AS start_date,
    DATE_ADD(DATE_SUB(STR_TO_DATE(LEFT(TRIM(`date_created`), 10), '%Y-%m-%d'), INTERVAL WEEKDAY(STR_TO_DATE(LEFT(TRIM(`date_created`), 10), '%Y-%m-%d')) DAY), INTERVAL 6 DAY) AS end_date,
    TRIM(REPLACE(REPLACE(REPLACE(REPLACE(`tracking_code`, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', '')) AS tracking_code
  FROM vl_data
  WHERE LEFT(TRIM(`date_created`), 10) BETWEEN '2026-01-01' AND '2026-12-31'
    AND LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(`tracking_code`, ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), '\\t', ''))) NOT IN ('', 'null', 'none', 'non')
  GROUP BY period_key, start_date, end_date, tracking_code
)
SELECT
  v.period_key,
  CONCAT(DATE_FORMAT(v.start_date, '%d %b'), ' - ', DATE_FORMAT(v.end_date, '%d %b')) AS period_label,
  v.start_date,
  v.end_date,
  COUNT(*) AS unique_vl_tracking_codes,
  SUM(CASE WHEN p.`barcode` IS NOT NULL THEN 1 ELSE 0 END) AS available_in_packages,
  SUM(CASE WHEN p.`barcode` IS NULL THEN 1 ELSE 0 END) AS exception_missing_in_packages,
  ROUND(SUM(CASE WHEN p.`barcode` IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS availability_percent,
  ROUND(SUM(CASE WHEN p.`barcode` IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS exception_percent
FROM weekly_unique_vl v
LEFT JOIN packages_unique_barcodes p ON p.`barcode` = v.`tracking_code` COLLATE utf8mb4_unicode_ci
GROUP BY v.period_key, v.start_date, v.end_date
ORDER BY v.period_key;

ALTER TABLE report_vl_monthly_availability ADD PRIMARY KEY (`period_key`);
ALTER TABLE report_vl_weekly_availability ADD PRIMARY KEY (`period_key`);
