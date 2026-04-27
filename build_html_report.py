from __future__ import annotations

import csv
import html
import subprocess
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_HTML = BASE_DIR / "index.html"
DB_NAME = "cphl_review"

MYSQL_COMMAND = [
    "mariadb",
    "-h",
    "127.0.0.1",
    "-P",
    "3306",
    "-u",
    "root",
    DB_NAME,
    "--batch",
    "--raw",
    "--skip-column-names",
]


def query_rows(sql: str) -> list[list[str]]:
    command = ["env", "MYSQL_PWD=root", *MYSQL_COMMAND, "-e", sql]
    result = subprocess.run(command, check=True, text=True, capture_output=True)
    return list(csv.reader(result.stdout.splitlines(), delimiter="\t"))


def query_one(sql: str) -> list[str]:
    rows = query_rows(sql)
    return rows[0] if rows else []


def cell(value: object) -> str:
    return html.escape(str(value or ""))


def fmt_int(value: str | int | float) -> str:
    return f"{int(float(value)):,}"


def fmt_pct(value: str | int | float) -> str:
    return f"{float(value):.2f}%"


def month_label(source_file: str) -> str:
    labels = {
        "202602.csv": "Feb 2026",
        "202603.csv": "Mar 2026",
        "202604.csv": "Apr 2026",
    }
    return labels.get(source_file, source_file)


def pct_color(value: str | float) -> str:
    v = float(value)
    if v >= 97:
        return "#16a34a"
    if v >= 90:
        return "#d97706"
    return "#dc2626"


def inclusion_chart(rows: list[list[str]]) -> str:
    width = 800
    height = 260
    left = 60
    right = 30
    top = 30
    bottom = 50
    plot_width = width - left - right
    plot_height = height - top - bottom
    values = [float(row[6]) for row in rows]
    labels = [row[0] for row in rows]
    y_min = max(0.0, min(values) - 4.0)
    y_max = min(100.0, max(values) + 2.0)
    if y_max - y_min < 1:
        y_min = max(0.0, y_min - 1)
        y_max = min(100.0, y_max + 1)

    x_positions = [
        left + (plot_width * i / max(1, len(values) - 1))
        for i in range(len(values))
    ]
    y_positions = [
        top + plot_height - ((v - y_min) / (y_max - y_min) * plot_height)
        for v in values
    ]

    path_d = " ".join(
        f"{'M' if i == 0 else 'L'} {x:.2f} {y:.2f}"
        for i, (x, y) in enumerate(zip(x_positions, y_positions))
    )
    area_d = (
        f"M {x_positions[0]:.2f} {top + plot_height} "
        + path_d.lstrip("M ")
        + f" L {x_positions[-1]:.2f} {top + plot_height} Z"
    )

    grid = []
    for step in range(5):
        v = y_min + ((y_max - y_min) * step / 4)
        y = top + plot_height - (plot_height * step / 4)
        grid.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{width - right}" y2="{y:.2f}" '
            f'stroke="#e2e8f0" stroke-dasharray="4 3" />'
            f'<text x="{left - 8}" y="{y + 4:.2f}" text-anchor="end" '
            f'fill="#94a3b8" font-size="11" font-family="system-ui,sans-serif">{v:.1f}%</text>'
        )

    points = []
    for label, value, x, y in zip(labels, values, x_positions, y_positions):
        lbl_y = y - 16 if y > top + 24 else y + 28
        col = pct_color(value)
        points.append(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="7" fill="white" stroke="{col}" stroke-width="2.5" />'
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="3.5" fill="{col}" />'
            f'<text x="{x:.2f}" y="{lbl_y:.2f}" text-anchor="middle" '
            f'fill="{col}" font-size="12" font-weight="700" font-family="system-ui,sans-serif">{value:.2f}%</text>'
            f'<text x="{x:.2f}" y="{height - 12}" text-anchor="middle" '
            f'fill="#64748b" font-size="12" font-family="system-ui,sans-serif">{cell(label)}</text>'
        )

    return f"""
    <svg viewBox="0 0 {width} {height}" role="img" aria-label="Monthly inclusion trend" style="width:100%;height:auto;display:block;">
      <defs>
        <linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="#188568" stop-opacity="0.18" />
          <stop offset="100%" stop-color="#188568" stop-opacity="0.01" />
        </linearGradient>
      </defs>
      {''.join(grid)}
      <line x1="{left}" y1="{top + plot_height}" x2="{width - right}" y2="{top + plot_height}" stroke="#cbd5e1" stroke-width="1.5" />
      <line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#cbd5e1" stroke-width="1.5" />
      <path d="{area_d}" fill="url(#areaGrad)" />
      <path d="{path_d}" fill="none" stroke="#188568" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" />
      {''.join(points)}
    </svg>
    """


def build_report() -> str:
    overall = query_one(
        """
        SELECT
          total_unique_vl_tracking_codes,
          available_in_packages,
          exception_missing_in_packages,
          availability_percent,
          exception_percent,
          vl_start_date,
          vl_end_date
        FROM report_vl_overall_availability
        """
    )
    raw_counts = query_one(
        """
        SELECT
          (SELECT COUNT(*) FROM vl_data) AS vl_data_rows,
          (SELECT COUNT(*) FROM packages) AS package_rows,
          (SELECT COUNT(*) FROM packages_unique_barcodes) AS package_unique
        """
    )
    monthly_rows = query_rows(
        """
        SELECT
          period_label,
          start_date,
          end_date,
          unique_vl_tracking_codes,
          available_in_packages,
          exception_missing_in_packages,
          availability_percent,
          exception_percent
        FROM report_vl_monthly_availability
        ORDER BY period_key
        """
    )
    cleaned_count_rows = query_rows(
        """
        SELECT dataset, period_label, original_rows, rows_with_clean_key, unique_clean_items
        FROM report_original_cleaned_counts
        WHERE dataset = 'VL Data'
        ORDER BY period_key
        """
    )
    top_duplicate_rows = query_rows(
        """
        SELECT COUNT(tracking_code) AS counts, tracking_code
        FROM vl_data
        GROUP BY tracking_code
        HAVING COUNT(tracking_code) > 1
        """
    )
    exception_rows = query_rows(
        """
        SELECT tracking_code, source_file, date_created, date_received, facility, district
        FROM report_vl_exception_list
        ORDER BY tracking_code
        """
    )

    (
        total_vl,
        available,
        exceptions,
        availability_percent,
        exception_percent,
        vl_start_date,
        vl_end_date,
    ) = overall
    vl_data_rows, package_rows, package_unique = raw_counts

    generated_at = datetime.now().astimezone().strftime("%d %b %Y, %H:%M %Z")
    chart = inclusion_chart(monthly_rows)

    # ── Timeline nodes ───────────────────────────────────────────────────────
    n = len(monthly_rows)
    timeline_nodes = []
    for i, row in enumerate(monthly_rows):
        pct = float(row[6])
        col = pct_color(pct)
        is_last = i == n - 1
        connector = "" if is_last else (
            '<div class="tl-connector">'
            '<div class="tl-line"></div>'
            '<svg class="tl-arrow" viewBox="0 0 10 16" width="10" height="16">'
            '<polyline points="1,1 9,8 1,15" fill="none" stroke="#cbd5e1" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>'
            '</svg>'
            '</div>'
        )
        node_html = f"""
        <div class="tl-item">
          <div class="tl-card">
            <div class="tl-header">
              <span class="tl-month">{cell(row[0])}</span>
              <span class="tl-pct" style="color:{col}">{fmt_pct(pct)}</span>
            </div>
            <div class="tl-bar-track">
              <div class="tl-bar-fill" style="width:{min(pct,100):.2f}%;background:{col}"></div>
            </div>
            <div class="tl-stats">
              <span class="tl-stat ok">&#10003; {fmt_int(row[4])} included</span>
              <span class="tl-stat err">&#10007; {fmt_int(row[5])} exceptions</span>
            </div>
            <div class="tl-range">{cell(row[1])} &mdash; {cell(row[2])}</div>
            <div class="tl-meta">{fmt_int(row[3])} unique VL codes</div>
          </div>
          {connector}
        </div>
        """
        timeline_nodes.append(node_html)

    # ── Data quality cards ────────────────────────────────────────────────────
    quality_cards = "\n".join(
        f"""
        <div class="dq-card">
          <p class="dq-month">{cell(row[1])}</p>
          <div class="dq-row">
            <span class="dq-label">Raw rows</span>
            <span class="dq-val">{fmt_int(row[2])}</span>
          </div>
          <div class="dq-row">
            <span class="dq-label">Clean rows</span>
            <span class="dq-val">{fmt_int(row[3])}</span>
          </div>
          <div class="dq-row">
            <span class="dq-label">Unique</span>
            <span class="dq-val highlight">{fmt_int(row[4])}</span>
          </div>
        </div>
        """
        for row in cleaned_count_rows
    )

    # ── Exception table rows ──────────────────────────────────────────────────
    exception_table = "\n".join(
        f"""
        <tr>
          <td><code>{cell(row[0])}</code></td>
          <td>{cell(month_label(row[1]))}</td>
          <td>{cell(row[2])}</td>
          <td>{cell(row[3])}</td>
          <td>{cell(row[4])}</td>
          <td>{cell(row[5])}</td>
        </tr>
        """
        for row in exception_rows
    )

    # ── Duplicate table rows ──────────────────────────────────────────────────
    top_duplicate_table = "\n".join(
        f"""
        <tr>
          <td><span class="badge">{fmt_int(row[0])}</span></td>
          <td><code>{cell(row[1])}</code></td>
        </tr>
        """
        for row in top_duplicate_rows
    )

    avail_pct = float(availability_percent)
    avail_col = pct_color(avail_pct)

    favicon = (
        "data:image/svg+xml,"
        "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E"
        "%3Crect width='32' height='32' rx='7' fill='%23188568'/%3E"
        "%3Crect x='5' y='21' width='5' height='6' rx='1.5' fill='white'/%3E"
        "%3Crect x='13.5' y='14' width='5' height='13' rx='1.5' fill='white'/%3E"
        "%3Crect x='22' y='7' width='5' height='20' rx='1.5' fill='white'/%3E"
        "%3C/svg%3E"
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>VL Inclusion Report &mdash; CPHL Uganda</title>
  <link rel="icon" href="{favicon}" />
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

    :root {{
      --bg:      #f1f5f9;
      --panel:   #ffffff;
      --ink:     #0f172a;
      --muted:   #64748b;
      --border:  #e2e8f0;
      --green:   #188568;
      --red:     #dc2626;
      --amber:   #d97706;
      --navy:    #0f2942;
      --radius:  10px;
    }}

    body {{
      background: var(--bg);
      color: var(--ink);
      font-family: "Inter", "Segoe UI", system-ui, sans-serif;
      font-size: 14px;
      line-height: 1.5;
    }}

    /* ── Top bar ───────────────────────────────────────── */
    .topbar {{
      background: var(--navy);
      color: white;
      padding: 0 28px;
      height: 58px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      position: sticky;
      top: 0;
      z-index: 100;
      box-shadow: 0 2px 8px rgba(0,0,0,.18);
    }}
    .topbar-left {{
      display: flex;
      align-items: center;
      gap: 12px;
    }}
    .topbar-icon {{
      background: var(--green);
      border-radius: 8px;
      width: 34px;
      height: 34px;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
    }}
    .topbar-title {{
      font-size: 16px;
      font-weight: 700;
      letter-spacing: -.01em;
    }}
    .topbar-sub {{
      font-size: 12px;
      opacity: .65;
      margin-top: 1px;
    }}
    .topbar-stamp {{
      font-size: 12px;
      opacity: .6;
      white-space: nowrap;
    }}

    /* ── Layout ────────────────────────────────────────── */
    main {{
      max-width: 1120px;
      margin: 0 auto;
      padding: 28px 24px 48px;
    }}

    .section-title {{
      font-size: 13px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .07em;
      color: var(--muted);
      margin-bottom: 14px;
      display: flex;
      align-items: center;
      gap: 8px;
    }}
    .section-title::after {{
      content: '';
      flex: 1;
      height: 1px;
      background: var(--border);
    }}

    .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 20px;
      margin-bottom: 20px;
    }}

    h2 {{
      font-size: 16px;
      font-weight: 700;
      margin-bottom: 4px;
    }}
    .panel-sub {{
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 16px;
    }}

    /* ── Notes list ────────────────────────────────────── */
    .notes-list {{
      margin: 0;
      padding-left: 20px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.8;
    }}
    .notes-list li {{ margin-bottom: 4px; }}
    .notes-list li:last-child {{ margin-bottom: 0; }}
    .pill-green {{
      display: inline-block;
      background: #dcfce7;
      color: #166534;
      font-size: 11px;
      font-weight: 700;
      padding: 1px 7px;
      border-radius: 99px;
    }}
    .pill-red {{
      display: inline-block;
      background: #fee2e2;
      color: #991b1b;
      font-size: 11px;
      font-weight: 700;
      padding: 1px 7px;
      border-radius: 99px;
    }}

    /* ── KPI cards ─────────────────────────────────────── */
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }}
    .kpi {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 16px;
    }}
    .kpi-label {{
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: .07em;
      color: var(--muted);
      margin-bottom: 8px;
    }}
    .kpi-value {{
      font-size: 28px;
      font-weight: 800;
      letter-spacing: -.02em;
      line-height: 1;
      margin-bottom: 6px;
    }}
    .kpi-desc {{
      font-size: 12px;
      color: var(--muted);
    }}
    .kpi-accent {{ border-top: 3px solid var(--green); }}
    .kpi-warn   {{ border-top: 3px solid var(--red); }}
    .col-green  {{ color: var(--green); }}
    .col-red    {{ color: var(--red); }}

    /* ── Timeline ──────────────────────────────────────── */
    .timeline {{
      display: flex;
      align-items: flex-start;
      gap: 0;
      overflow-x: auto;
      padding-bottom: 6px;
    }}
    .tl-item {{
      display: flex;
      align-items: center;
      flex: 1;
      min-width: 200px;
    }}
    .tl-card {{
      flex: 1;
      background: #f8fafc;
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 16px;
      position: relative;
    }}
    .tl-header {{
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      margin-bottom: 10px;
    }}
    .tl-month {{
      font-size: 13px;
      font-weight: 700;
      color: var(--ink);
    }}
    .tl-pct {{
      font-size: 22px;
      font-weight: 800;
      letter-spacing: -.02em;
    }}
    .tl-bar-track {{
      height: 6px;
      background: var(--border);
      border-radius: 99px;
      margin-bottom: 12px;
      overflow: hidden;
    }}
    .tl-bar-fill {{
      height: 100%;
      border-radius: 99px;
      transition: width .4s ease;
    }}
    .tl-stats {{
      display: flex;
      gap: 10px;
      margin-bottom: 8px;
      flex-wrap: wrap;
    }}
    .tl-stat {{
      font-size: 12px;
      font-weight: 600;
      padding: 2px 8px;
      border-radius: 99px;
    }}
    .tl-stat.ok  {{ background: #dcfce7; color: #166534; }}
    .tl-stat.err {{ background: #fee2e2; color: #991b1b; }}
    .tl-range {{
      font-size: 11px;
      color: var(--muted);
    }}
    .tl-meta {{
      font-size: 11px;
      color: var(--muted);
      margin-top: 2px;
    }}
    .tl-connector {{
      display: flex;
      align-items: center;
      flex-shrink: 0;
      padding: 0 4px;
    }}
    .tl-line {{
      height: 2px;
      width: 24px;
      background: var(--border);
    }}
    .tl-arrow {{
      opacity: .5;
    }}

    /* ── Data quality ──────────────────────────────────── */
    .dq-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }}
    .dq-card {{
      background: #f8fafc;
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 16px;
    }}
    .dq-month {{
      font-size: 13px;
      font-weight: 700;
      margin-bottom: 12px;
      color: var(--ink);
    }}
    .dq-row {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 6px 0;
      border-bottom: 1px solid var(--border);
    }}
    .dq-row:last-child {{ border-bottom: none; }}
    .dq-label {{ font-size: 12px; color: var(--muted); }}
    .dq-val   {{ font-size: 13px; font-weight: 600; }}
    .dq-val.highlight {{ color: var(--green); }}

    /* ── Chart ─────────────────────────────────────────── */
    .chart-wrap {{
      overflow-x: auto;
      border-radius: 8px;
      background: #f8fafc;
      border: 1px solid var(--border);
      padding: 12px 8px 4px;
    }}

    /* ── Tables ────────────────────────────────────────── */
    .scroll-table {{
      overflow: auto;
      border: 1px solid var(--border);
      border-radius: 8px;
      max-height: 400px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th {{
      background: #f8fafc;
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .07em;
      padding: 10px 12px;
      text-align: left;
      border-bottom: 1px solid var(--border);
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    td {{
      padding: 9px 12px;
      border-bottom: 1px solid var(--border);
      vertical-align: middle;
    }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: #f8fafc; }}

    code {{
      background: #eef3f7;
      padding: 2px 6px;
      border-radius: 4px;
      font-family: "SFMono-Regular", "Fira Code", Menlo, monospace;
      font-size: 12px;
      color: #1e40af;
    }}
    .badge {{
      display: inline-block;
      background: #fef3c7;
      color: #92400e;
      font-weight: 700;
      font-size: 12px;
      padding: 2px 8px;
      border-radius: 99px;
    }}

    /* ── Responsive ────────────────────────────────────── */
    @media (max-width: 860px) {{
      .kpi-grid {{ grid-template-columns: repeat(2, minmax(0,1fr)); }}
      .dq-grid  {{ grid-template-columns: repeat(1, minmax(0,1fr)); }}
      .timeline {{ flex-direction: column; }}
      .tl-item  {{ flex-direction: column; width: 100%; min-width: unset; }}
      .tl-connector {{ transform: rotate(90deg); padding: 4px 0; }}
    }}
  </style>
</head>
<body>

  <div class="topbar">
    <div class="topbar-left">
      <div class="topbar-icon">
        <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
          <rect x="2" y="13" width="3.5" height="5" rx="1" fill="white"/>
          <rect x="8.25" y="8" width="3.5" height="10" rx="1" fill="white"/>
          <rect x="14.5" y="3" width="3.5" height="15" rx="1" fill="white"/>
        </svg>
      </div>
      <div>
        <div class="topbar-title">VL Inclusion Report</div>
        <div class="topbar-sub">CPHL Uganda &mdash; {cell(vl_start_date)} to {cell(vl_end_date)}</div>
      </div>
    </div>
    <div class="topbar-stamp">Generated {cell(generated_at)}</div>
  </div>

  <main>

    <!-- Notes -->
    <p class="section-title">Notes</p>
    <div class="panel">
      <ul class="notes-list">
        <li>Data was exported from the <strong>VL Dashboard</strong> for February, March, and mid-April 2026.</li>
        <li>It was compared against sample tracking records in <strong>ResTrack</strong> (packages table).</li>
        <li>All VL Dashboard entries are expected to exist in ResTrack &mdash; every VL sample dispatched from a facility should have a corresponding barcode in ResTrack.</li>
        <li>The reverse is not expected: ResTrack holds broader logistics data beyond VL, so ResTrack records with no VL match are normal and not treated as errors.</li>
        <li>VL tracking codes were deduplicated per period before matching.</li>
        <li><span class="pill-green">Included</span> &mdash; VL tracking code found in ResTrack.</li>
        <li><span class="pill-red">Exception</span> &mdash; VL tracking code with no matching barcode in ResTrack, meaning that sample was not tracked through the logistics system.</li>
      </ul>
    </div>

    <!-- KPI Summary -->
    <p class="section-title">Summary</p>
    <div class="kpi-grid">
      <div class="kpi">
        <p class="kpi-label">VL Data Rows</p>
        <p class="kpi-value">{fmt_int(vl_data_rows)}</p>
        <p class="kpi-desc">Total rows in <code>vl_data</code></p>
      </div>
      <div class="kpi">
        <p class="kpi-label">Unique VL Codes</p>
        <p class="kpi-value">{fmt_int(total_vl)}</p>
        <p class="kpi-desc">Distinct tracking codes across all months</p>
      </div>
      <div class="kpi">
        <p class="kpi-label">Unique Barcodes</p>
        <p class="kpi-value">{fmt_int(package_unique)}</p>
        <p class="kpi-desc">Cleaned &amp; deduplicated</p>
      </div>
      <div class="kpi kpi-accent">
        <p class="kpi-label">Included VL Codes</p>
        <p class="kpi-value col-green">{fmt_int(available)}</p>
        <p class="kpi-desc">{fmt_pct(availability_percent)} inclusion rate</p>
      </div>
      <div class="kpi kpi-warn">
        <p class="kpi-label">Exceptions</p>
        <p class="kpi-value col-red">{fmt_int(exceptions)}</p>
        <p class="kpi-desc">{fmt_pct(exception_percent)} missing from packages</p>
      </div>
    </div>

    <!-- Monthly Timeline -->
    <p class="section-title">Monthly Performance Timeline</p>
    <div class="panel">
      <div class="timeline">
        {''.join(timeline_nodes)}
      </div>
    </div>

    <!-- Inclusion Trend Chart -->
    <p class="section-title">Inclusion Trend</p>
    <div class="panel">
      <h2>Month-on-Month Inclusion %</h2>
      <p class="panel-sub">Percentage of unique VL tracking codes found in the packages system, per period.</p>
      <div class="chart-wrap">{chart}</div>
    </div>

    <!-- Data Quality -->
    <p class="section-title">Data Quality</p>
    <div class="panel">
      <h2>Original vs Cleaned VL Counts</h2>
      <p class="panel-sub">Raw row counts per import file, after deduplication and key cleaning.</p>
      <div class="dq-grid">{quality_cards}</div>
    </div>

    <!-- Duplicates -->
    <p class="section-title">Duplicate Tracking Codes</p>
    <div class="panel">
      <h2>Tracking Codes Appearing More Than Once</h2>
      <p class="panel-sub">All tracking codes with a count &gt; 1 in the raw VL data.</p>
      <div class="scroll-table">
        <table>
          <thead>
            <tr><th>Occurrences</th><th>Tracking Code</th></tr>
          </thead>
          <tbody>{top_duplicate_table}</tbody>
        </table>
      </div>
    </div>

    <!-- Exception List -->
    <p class="section-title" id="exceptions">Exception List</p>
    <div class="panel">
      <h2>VL Codes Not Found in Packages</h2>
      <p class="panel-sub">{fmt_int(exceptions)} unique VL tracking codes have no matching barcode in the packages system.</p>
      <div class="scroll-table">
        <table>
          <thead>
            <tr>
              <th>Tracking Code</th>
              <th>Month</th>
              <th>Date Created</th>
              <th>Date Received</th>
              <th>Facility</th>
              <th>District</th>
            </tr>
          </thead>
          <tbody>{exception_table}</tbody>
        </table>
      </div>
    </div>

  </main>
</body>
</html>
"""


def main() -> None:
    OUTPUT_HTML.write_text(build_report(), encoding="utf-8")
    print(f"Wrote {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
