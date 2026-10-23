import hashlib
import math
import os
import re
from collections import Counter
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    LongTable,
    TableStyle,
)
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import (
    HorizontalBarChart,
    VerticalBarChart,
)


BUCKET_NAME = "inunity-log-analytics-dev"
REPORT_S3_KEY = "reports/inunity_log_analytics_report.pdf"
TOP_N = 20


def parse_timestamp(value):
    """Parse an ISO timestamp safely."""
    if not value:
        return None

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def normalize_path(path):
    """Replace variable IDs so requests to the same API can be grouped."""
    path = str(path or "UNKNOWN").split("?")[0]

    # UUIDs
    path = re.sub(
        r"(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
        r"[0-9a-f]{4}-[0-9a-f]{12}\b",
        "{id}",
        path,
    )

    # Long hexadecimal IDs
    path = re.sub(r"(?i)(?<=/)[0-9a-f]{24,}(?=/|$)", "{id}", path)

    # Numeric IDs in path segments
    path = re.sub(r"(?<=/)\d+(?=/|$)", "{id}", path)

    return path


def identify_feature(path):
    """Assign API paths to broad application feature categories."""
    path = path.lower()

    rules = [
        ("Notifications", ("notification",)),
        ("Tokens", ("token", "fcm")),
        ("User Management", ("user", "profile", "interest", "favorite")),
        (
            "Academic Structure",
            (
                "department", "academic-year", "academic-years",
                "classroom", "course", "semester", "program",
                "batch", "role", "instructor",
            ),
        ),
        ("Attendance", ("attendance",)),
        ("Assignments", ("assignment", "deadline", "submission")),
        ("Events", ("event", "calendar")),
        ("Internships", ("internship",)),
        ("Admissions", ("admission",)),
        ("Recruitment", ("recruitment", "job", "career")),
        ("Skillbuilder", ("skillbuilder",)),
        ("CIES", ("cie", "exam", "assessment")),
        ("Circulars", ("circular",)),
        ("Authentication", ("auth", "login", "logout", "session")),
        ("Timetable", ("time-table", "timetable", "schedule")),
        ("GitHub", ("github",)),
        ("Schemes", ("scheme",)),
        ("Rubrics", ("rubric",)),
    ]

    for feature, keywords in rules:
        if any(keyword in path for keyword in keywords):
            return feature

    return "Other"


def analyze_logs(records):
    """Calculate request, client, feature, timeline, and spike statistics."""
    if not records:
        raise ValueError("No records available for analysis.")

    total_requests = len(records)

    method_counts = Counter()
    operation_counts = Counter()
    path_counts = Counter()
    feature_counts = Counter()
    client_counts = Counter()
    hourly_traffic = Counter()
    hour_of_day_counts = Counter()
    minute_operation_counts = Counter()
    timestamps = []

    for record in records:
        method = str(record.get("method") or "UNKNOWN").upper()
        original_path = str(record.get("path") or "UNKNOWN")
        normalized_path = normalize_path(original_path)

        method_counts[method] += 1
        path_counts[original_path] += 1

        operation = f"{method.lower()} {normalized_path}"
        operation_counts[operation] += 1
        feature_counts[identify_feature(normalized_path)] += 1

        # The transformation stage may already have hashed remote_addr.
        client = str(record.get("remote_addr") or "").strip()

        if client:
            if ":" in client or re.fullmatch(
                r"\d{1,3}(?:\.\d{1,3}){3}", client
            ):
                client = hashlib.sha256(client.encode("utf-8")).hexdigest()

            client_counts[client] += 1

        timestamp = parse_timestamp(record.get("timestamp"))

        if timestamp is not None:
            timestamps.append(timestamp)

            hour_key = timestamp.strftime("%Y-%m-%d %H:00")
            hourly_traffic[hour_key] += 1
            hour_of_day_counts[timestamp.hour] += 1

            # Count requests per minute for each API operation.
            minute_key = timestamp.strftime("%Y-%m-%d %H:%M")
            minute_operation_counts[(minute_key, operation)] += 1

    # Compare operations across all observed minutes.
    all_minutes = sorted({
        minute for minute, _ in minute_operation_counts
    })

    spike_candidates = []

    for operation in operation_counts:
        counts = [
            minute_operation_counts.get((minute, operation), 0)
            for minute in all_minutes
        ]

        if not counts:
            continue

        average_per_minute = sum(counts) / len(counts)
        variance = sum(
            (count - average_per_minute) ** 2 for count in counts
        ) / len(counts)
        standard_deviation = math.sqrt(variance)

        threshold = max(10, average_per_minute + 3 * standard_deviation)

        for minute, count in zip(all_minutes, counts):
            if count > threshold:
                date_part, time_part = minute.split(" ")
                hour, minute_number = time_part.split(":")

                spike_candidates.append({
                    "date": date_part,
                    "hour": int(hour),
                    "minute": int(minute_number),
                    "operation": operation,
                    "requests": count,
                    "threshold": round(threshold, 2),
                })

    spike_candidates.sort(
        key=lambda item: item["requests"],
        reverse=True,
    )
    spike_candidates = spike_candidates[:20]

    top_operations = dict(operation_counts.most_common(TOP_N))

    results = {
        "total_requests": total_requests,
        "method_counts": dict(method_counts.most_common()),
        "unique_api_operations": len(operation_counts),
        "unique_clients": len(client_counts),
        "unique_features": len(feature_counts),
        "start_date": (
            min(timestamps).strftime("%Y-%m-%d") if timestamps else "Unknown"
        ),
        "end_date": (
            max(timestamps).strftime("%Y-%m-%d") if timestamps else "Unknown"
        ),
        "top_operations": top_operations,
        "operation_counts": dict(operation_counts),
        "traffic_percentages": {
            operation: round(count / total_requests * 100, 2)
            for operation, count in top_operations.items()
        },
        "path_counts": dict(path_counts.most_common(TOP_N)),
        "hourly_traffic": dict(sorted(hourly_traffic.items())),
        "hour_of_day": dict(sorted(hour_of_day_counts.items())),
        "top_clients": dict(client_counts.most_common(TOP_N)),
        "spike_candidates": spike_candidates,
        "feature_usage": dict(feature_counts.most_common()),
    }

    print("\n--- InUnity Log Analytics ---")
    print("Total requests:", results["total_requests"])
    print("Unique API operations:", results["unique_api_operations"])
    print("Unique clients:", results["unique_clients"])
    print("Unique features:", results["unique_features"])
    print("Date range:", results["start_date"], "to", results["end_date"])

    return results


def generate_pdf_report(results, pdf_path):
    """Generate the PDF report from the analysis results."""
    os.makedirs(os.path.dirname(pdf_path) or ".", exist_ok=True)

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name="SmallCell",
        parent=styles["BodyText"],
        fontSize=7,
        leading=9,
        wordWrap="CJK",
    ))
    styles.add(ParagraphStyle(
        name="SectionHeading",
        parent=styles["Heading2"],
        textColor=colors.HexColor("#24476B"),
        spaceBefore=12,
        spaceAfter=7,
    ))
    styles.add(ParagraphStyle(
        name="ReportSubtitle",
        parent=styles["Normal"],
        alignment=TA_CENTER,
        textColor=colors.HexColor("#555555"),
        fontSize=9,
    ))

    document = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        rightMargin=35,
        leftMargin=35,
        topMargin=35,
        bottomMargin=35,
        title="InUnity Log Analytics Report",
        author="InUnity Log Analytics Pipeline",
    )

    story = []
    page_width = A4[0] - 70

    story.append(Paragraph("InUnity Log Analytics Report", styles["Title"]))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        f"Analysis period: {results['start_date']} to {results['end_date']}",
        styles["ReportSubtitle"],
    ))
    story.append(Spacer(1, 16))

    def add_table(title, headers, rows, widths=None):
        story.append(Paragraph(title, styles["SectionHeading"]))

        if not rows:
            story.append(Paragraph(
                "No data available for this section.",
                styles["BodyText"],
            ))
            story.append(Spacer(1, 8))
            return

        table_data = [headers]
        for row in rows:
            table_data.append([
                Paragraph(str(value), styles["SmallCell"]) for value in row
            ])

        if widths is None:
            widths = [page_width / len(headers)] * len(headers)

        table = LongTable(
            table_data,
            colWidths=widths,
            repeatRows=1,
            hAlign="LEFT",
        )
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#24476B")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#CCCCCC")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
            ("FONTSIZE", (0, 0), (-1, -1), 7),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [
                colors.white, colors.HexColor("#F3F6F9")
            ]),
        ]))

        story.append(table)
        story.append(Spacer(1, 10))

    def add_horizontal_chart(title, items, max_items=10):
        # Select the highest-count items first.
        items = sorted(
            items.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:max_items]

        if not items:
            return

        story.append(Paragraph(title, styles["SectionHeading"]))

        # Reverse so the highest item appears at the top of the chart.
        items = items
        labels = [str(label)[:42] for label, _ in items]
        values = [int(value) for _, value in items]

        drawing_height = max(170, len(items) * 22 + 40)
        drawing = Drawing(page_width, drawing_height)

        chart = HorizontalBarChart()
        chart.x = 175
        chart.y = 25
        chart.width = page_width - 195
        chart.height = drawing_height - 55
        chart.data = [values]
        chart.categoryAxis.categoryNames = labels
        chart.categoryAxis.labels.fontSize = 7
        chart.categoryAxis.labels.dx = -3
        chart.categoryAxis.labels.dy = 0
        chart.valueAxis.labels.fontSize = 7
        chart.valueAxis.valueMin = 0
        chart.valueAxis.valueMax = max(values) * 1.15 if max(values) else 1
        chart.valueAxis.valueStep = max(1, int(chart.valueAxis.valueMax / 5))
        chart.bars[0].fillColor = colors.HexColor("#3676A8")
        chart.barWidth = 10

        drawing.add(chart)
        story.append(drawing)
        story.append(Spacer(1, 8))

    def add_vertical_chart(title, labels, values):
        if not labels or not values:
            return

        story.append(Paragraph(title, styles["SectionHeading"]))

        drawing = Drawing(page_width, 210)
        chart = VerticalBarChart()
        chart.x = 40
        chart.y = 45
        chart.width = page_width - 65
        chart.height = 135
        chart.data = [values]
        chart.categoryAxis.categoryNames = [str(x) for x in labels]
        chart.categoryAxis.labels.fontSize = 7
        chart.categoryAxis.labels.angle = 0
        chart.categoryAxis.labels.boxAnchor = "ne"
        chart.valueAxis.labels.fontSize = 7
        chart.valueAxis.valueMin = 0
        chart.valueAxis.valueMax = max(values) * 1.15 if max(values) else 1
        chart.valueAxis.valueStep = max(1, int(chart.valueAxis.valueMax / 5))
        chart.bars[0].fillColor = colors.HexColor("#3676A8")
        chart.barWidth = max(
            4, min(14, chart.width / max(len(values) * 1.6, 1))
        )

        drawing.add(chart)
        story.append(drawing)
        story.append(Spacer(1, 8))

    total_requests = results["total_requests"]

    # 1. Overall summary
    add_table(
        "1. Overall Summary",
        ["Metric", "Value"],
        [
            ["Total Requests", total_requests],
            ["Unique API Operations", results["unique_api_operations"]],
            ["Unique Clients", results["unique_clients"]],
            ["Unique Features", results["unique_features"]],
            ["Start Date", results["start_date"]],
            ["End Date", results["end_date"]],
        ],
        [page_width * 0.65, page_width * 0.35],
    )

    # 2. HTTP methods
    add_table(
        "2. Request Type Distribution",
        ["HTTP Method", "Request Count", "Traffic (%)"],
        [
            [method, count, f"{count / total_requests * 100:.2f}"]
            for method, count in results["method_counts"].items()
        ],
        [page_width * 0.35, page_width * 0.35, page_width * 0.30],
    )

    add_vertical_chart(
        "HTTP Method Distribution",
        list(results["method_counts"].keys()),
        list(results["method_counts"].values()),
    )

    # 3. Top API operations, sorted by request count.
    sorted_operations = sorted(
        results["top_operations"].items(),
        key=lambda item: item[1],
        reverse=True,
    )

    add_table(
        "3. Top API Operations",
        ["Operation", "Requests", "Traffic (%)"],
        [
            [operation, count, f"{count / total_requests * 100:.2f}"]
            for operation, count in sorted_operations
        ],
        [page_width * 0.60, page_width * 0.20, page_width * 0.20],
    )

    add_horizontal_chart(
        "Top API Operations by Request Count",
        results["top_operations"],
        10,
    )

    # 4. Traffic by hour of day
    hour_of_day = results["hour_of_day"]

    add_vertical_chart(
        "4. Traffic by Hour of Day",
        list(hour_of_day.keys()),
        list(hour_of_day.values()),
    )

    add_table(
        "Hourly Traffic Summary",
        ["Hour of Day", "Request Count"],
        [
            [f"{int(hour):02d}:00–{int(hour):02d}:59", count]
            for hour, count in hour_of_day.items()
        ],
        [page_width * 0.65, page_width * 0.35],
    )

    # 5. Top clients, sorted by request count.
    sorted_clients = sorted(
        results["top_clients"].items(),
        key=lambda item: item[1],
        reverse=True,
    )

    add_table(
        "5. Top Clients (Hashed Addresses)",
        ["Client Hash", "Request Count"],
        [[client, count] for client, count in sorted_clients],
        [page_width * 0.75, page_width * 0.25],
    )

    # 6. Endpoint spike candidates
    spike_rows = [
        [
            item["date"],
            item["hour"],
            item["minute"],
            item["operation"],
            item["requests"],
            item["threshold"],
        ]
        for item in results["spike_candidates"]
    ]

    add_table(
        "6. Endpoint Spike Candidates",
        ["Date", "Hour", "Minute", "Operation", "Requests", "Threshold"],
        spike_rows,
        [
            page_width * 0.14,
            page_width * 0.07,
            page_width * 0.08,
            page_width * 0.43,
            page_width * 0.13,
            page_width * 0.15,
        ],
    )

    # 7. Feature usage, sorted by request count.
    sorted_features = sorted(
        results["feature_usage"].items(),
        key=lambda item: item[1],
        reverse=True,
    )

    add_table(
        "7. Feature Usage",
        ["Feature", "Request Count", "Traffic (%)"],
        [
            [feature, count, f"{count / total_requests * 100:.2f}"]
            for feature, count in sorted_features
        ],
        [page_width * 0.50, page_width * 0.25, page_width * 0.25],
    )

    add_horizontal_chart(
        "Top Features by Request Count",
        results["feature_usage"],
        10,
    )

    # 8. Full hourly timeline
    add_table(
        "8. Hourly Traffic by Date",
        ["Date and Hour", "Request Count"],
        list(results["hourly_traffic"].items()),
        [page_width * 0.65, page_width * 0.35],
    )

    document.build(story)
    print(f"PDF report generated: {pdf_path}")


def upload_pdf_to_s3(pdf_path):
    """Upload the generated report to the configured S3 key."""
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_hook.load_file(
        filename=pdf_path,
        key=REPORT_S3_KEY,
        bucket_name=BUCKET_NAME,
        replace=True,
    )
    print(f"PDF uploaded to s3://{BUCKET_NAME}/{REPORT_S3_KEY}")


if __name__ == "__main__":
    print("Analytics module is ready.")