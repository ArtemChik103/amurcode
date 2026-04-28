from __future__ import annotations

import csv
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "case"
STATIC_DIR = ROOT / "static"
RAG_DIR = ROOT / "docs" / "rag"
QUALITY_ISSUES: list[dict] = []
LOAD_STATS: dict[str, dict[str, int]] = {}
METRIC_KEYS = ["limit", "obligation", "cash", "agreement", "contract", "payment", "buau"]
METRICS = {
    "limit": "Лимиты",
    "obligation": "БО",
    "cash": "Касса",
    "agreement": "Соглашения",
    "contract": "Контракты",
    "payment": "Платежи",
    "buau": "БУ/АУ",
}
TEMPLATES = {
    "all": {"label": "Все данные", "description": "Без предметного шаблона"},
    "kik": {"label": "КИК", "description": "КЦСР содержит 978 с 6-й позиции"},
    "skk": {"label": "СКК", "description": "КЦСР содержит 6105 с 6-й позиции"},
    "two_thirds": {"label": "2/3", "description": "КЦСР содержит 970 с 6-й позиции"},
    "okv": {"label": "ОКВ", "description": "Капитальные вложения по КВР"},
}
QUICK_ACTIONS = {
    "show_skk": {
        "label": "Показать СКК",
        "description": "Сведения по специальным казначейским кредитам",
        "mode": "slice",
        "template": "skk",
        "metrics": ["limit", "obligation", "cash", "agreement", "contract", "payment", "buau"],
    },
    "show_kik": {
        "label": "Показать КИК",
        "description": "Инфраструктурные кредиты",
        "mode": "slice",
        "template": "kik",
        "metrics": ["limit", "obligation", "cash", "agreement", "contract", "payment"],
    },
    "show_two_thirds": {
        "label": "Показать 2/3",
        "description": "Высвобождаемые средства",
        "mode": "slice",
        "template": "two_thirds",
        "metrics": ["limit", "obligation", "cash", "agreement"],
    },
    "show_okv": {
        "label": "Показать ОКВ",
        "description": "Объекты капитальных вложений",
        "mode": "slice",
        "template": "okv",
        "metrics": ["limit", "obligation", "cash", "contract", "payment", "buau"],
    },
    "compare_skk": {
        "label": "Сравнить СКК",
        "description": "Что изменилось по СКК между первой и последней датой",
        "mode": "compare",
        "template": "skk",
        "metrics": ["limit", "obligation", "cash"],
    },
    "execution_problems": {
        "label": "Где проблемы с исполнением",
        "description": "Лимиты есть, а касса низкая или отсутствует",
        "mode": "slice",
        "template": "all",
        "metrics": ["limit", "cash", "payment", "buau"],
        "post_filter": "low_execution",
    },
}
ASSISTANT_INTENTS = {
    "run_query",
    "run_compare",
    "explain_metric",
    "explain_template",
    "find_object",
    "show_execution_problems",
    "show_source",
    "help",
}
CAPITAL_KVR = {"400", "410", "411", "412", "413", "414", "415", "416", "417"}

SNAPSHOT_RE = re.compile(r"на\s+(\d{2}\.\d{2}\.\d{4})")
MONTHS = {
    "январь": 1,
    "февраль": 2,
    "март": 3,
    "апрель": 4,
    "май": 5,
    "июнь": 6,
    "июль": 7,
    "август": 8,
    "сентябрь": 9,
    "октябрь": 10,
    "ноябрь": 11,
    "декабрь": 12,
}


def relative_source(path: Path | str) -> str:
    try:
        return str(Path(path).resolve().relative_to(ROOT)).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def add_quality_issue(
    source_file: str,
    source_row: int | str,
    severity: str,
    code: str,
    message: str,
    field: str = "",
    value: object = "",
) -> None:
    QUALITY_ISSUES.append(
        {
            "source_file": source_file,
            "source_row": source_row,
            "severity": severity,
            "code": code,
            "message": message,
            "field": field,
            "value": "" if value is None else str(value),
        }
    )


def parse_amount(value: object, source_file: str = "", source_row: int | str = "", field: str = "") -> float:
    if value is None:
        return 0.0
    text = str(value).strip().replace("\xa0", " ")
    if not text:
        return 0.0
    text = text.replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        if source_file or field:
            add_quality_issue(
                source_file,
                source_row,
                "warning",
                "amount_parse_failed",
                "Не удалось распарсить сумму",
                field,
                value,
            )
        return 0.0


def parse_date(value: object, source_file: str = "", source_row: int | str = "", field: str = "") -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    for fmt in ("%d.%m.%Y", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    if source_file or field:
        add_quality_issue(
            source_file,
            source_row,
            "warning",
            "date_parse_failed",
            "Не удалось распарсить дату",
            field,
            value,
        )
    return ""


def normalize_code(value: object) -> str:
    return re.sub(r"[^0-9A-Za-zА-Яа-я]", "", str(value or "")).upper()


def display_code(value: object) -> str:
    return str(value or "").strip()


def make_record(records: list[dict], source_file: str, source_row: int, raw: dict, **fields: object) -> dict:
    record = {
        "id": f"r{len(records) + 1}",
        "source_file": source_file,
        "source_row": source_row,
        "raw": dict(raw),
    }
    record.update(fields)
    return record


def selected_metrics(params: dict[str, list[str]] | None = None) -> list[str]:
    if not params:
        return list(METRIC_KEYS)
    raw = params.get("metrics", [""])[0].strip()
    if not raw:
        return list(METRIC_KEYS)
    result = [metric for metric in raw.split(",") if metric in METRIC_KEYS]
    return result or list(METRIC_KEYS)


def default_date_range() -> tuple[str, str]:
    snapshots = STORE.meta.get("snapshots", []) if "STORE" in globals() else []
    if not snapshots:
        return "", ""
    return snapshots[0], snapshots[-1]


def quick_actions_payload() -> list[dict]:
    return [{"code": code, **action} for code, action in QUICK_ACTIONS.items()]


def matches_template(record: dict, template: str) -> bool:
    if not template or template == "all":
        return True
    code = record.get("object_code_norm", "")
    if template == "kik":
        return code[5:8] == "978"
    if template == "skk":
        return code[5:9] == "6105"
    if template == "two_thirds":
        return code[5:8] == "970"
    if template == "okv":
        return normalize_code(record.get("kvr")) in CAPITAL_KVR
    return True


def rcb_snapshot_from_rows(rows: list[list[str]], fallback_name: str) -> str:
    for row in rows[:8]:
        joined = " ".join(row)
        match = SNAPSHOT_RE.search(joined)
        if match:
            return parse_date(match.group(1))
    lower = fallback_name.lower()
    for name, month in MONTHS.items():
        if name in lower:
            year_match = re.search(r"(20\d{2})", lower)
            year = int(year_match.group(1)) if year_match else 2025
            next_month = month + 1
            next_year = year
            if next_month == 13:
                next_month = 1
                next_year += 1
            return f"{next_year:04d}-{next_month:02d}-01"
    return ""


def agreement_snapshot(row: dict[str, str], filename: str) -> str:
    period = row.get("period_of_date", "")
    if " - " in period:
        return parse_date(period.split(" - ")[-1])
    digits = re.search(r"(\d{2})(\d{2})(20\d{2})", filename)
    if digits:
        day, month, year = digits.groups()
        return f"{year}-{month}-{day}"
    return ""


def buau_snapshot(filename: str) -> str:
    lower = filename.lower()
    year_match = re.search(r"(20\d{2})", lower)
    year = int(year_match.group(1)) if year_match else 2025
    for name, month in MONTHS.items():
        if name in lower:
            return f"{year:04d}-{month:02d}-01"
    return ""


@dataclass
class DataStore:
    records: list[dict]
    meta: dict


def load_data() -> DataStore:
    QUALITY_ISSUES.clear()
    LOAD_STATS.clear()
    records: list[dict] = []
    load_rcb(records)
    load_agreements(records)
    load_state_task(records)
    load_buau(records)
    enrich_records(records)

    budgets = sorted({r["budget"] for r in records if r.get("budget")})
    sources = sorted({r["source"] for r in records})
    snapshots = sorted({r["snapshot"] for r in records if r.get("snapshot")})
    objects = sorted(
        {r["object_name"] for r in records if r.get("object_name")},
        key=lambda x: x.lower(),
    )
    meta = {
        "records": len(records),
        "budgets": budgets,
        "sources": sources,
        "snapshots": snapshots,
        "objects": objects[:500],
        "load_stats": LOAD_STATS,
        "quality": quality_summary(),
    }
    return DataStore(records=records, meta=meta)


def enrich_records(records: list[dict]) -> None:
    source_counts: dict[str, int] = defaultdict(int)
    source_folders = {
        "РЧБ": "case/1_RCB",
        "Соглашения": "case/2_Agreements",
        "ГЗ: контракты": "case/3_StateTask",
        "ГЗ: платежи": "case/3_StateTask",
        "БУАУ": "case/4_BUAU_Export",
    }
    for index, record in enumerate(records, start=1):
        source = record.get("source", "")
        source_counts[source] += 1
        record.setdefault("id", f"r{index}")
        record.setdefault("source_file", source_folders.get(source, ""))
        record.setdefault("source_row", source_counts[source])
        record.setdefault("raw", {key: value for key, value in record.items() if key not in {"raw"}})
    for source, count in source_counts.items():
        LOAD_STATS[source or "unknown"] = {"read_rows": count, "records": count, "warnings": 0, "errors": 0}


def quality_summary() -> dict[str, int]:
    return {
        "warnings": sum(1 for issue in QUALITY_ISSUES if issue["severity"] == "warning"),
        "errors": sum(1 for issue in QUALITY_ISSUES if issue["severity"] == "error"),
    }


def start_load_stats(path: Path) -> dict[str, int]:
    key = relative_source(path)
    stats = {"read_rows": 0, "records": 0, "warnings": 0, "errors": 0}
    LOAD_STATS[key] = stats
    return stats


def finish_load_stats(stats: dict[str, int], issue_start: int) -> None:
    issues = QUALITY_ISSUES[issue_start:]
    stats["warnings"] = sum(1 for issue in issues if issue["severity"] == "warning")
    stats["errors"] = sum(1 for issue in issues if issue["severity"] == "error")


def load_rcb(records: list[dict]) -> None:
    folder = DATA_DIR / "1_RCB"
    for path in sorted(folder.glob("*.csv")):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.reader(handle, delimiter=";"))
        header_index = next(
            (i for i, row in enumerate(rows) if row and row[0].strip() == "Бюджет"),
            None,
        )
        if header_index is None:
            continue
        snapshot = rcb_snapshot_from_rows(rows, path.name)
        header = rows[header_index]
        for row in rows[header_index + 1 :]:
            if not any(cell.strip() for cell in row):
                continue
            item = dict(zip(header, row))
            kcsr = display_code(item.get("КЦСР"))
            object_name = item.get("Наименование КЦСР", "").strip() or kcsr
            budget = item.get("Бюджет", "").strip()
            records.append(
                {
                    "source": "РЧБ",
                    "snapshot": snapshot,
                    "event_date": parse_date(item.get("Дата проводки")),
                    "budget": budget,
                    "object_code": kcsr,
                    "object_code_norm": normalize_code(kcsr),
                    "object_name": object_name,
                    "kfsr": display_code(item.get("КФСР")),
                    "kvr": display_code(item.get("КВР")),
                    "kosgu": display_code(item.get("КОСГУ")),
                    "counterparty": item.get("Наименование КВСР", "").strip(),
                    "document_number": "",
                    "description": item.get("Наименование КВР", "").strip(),
                    "limit": parse_amount(item.get("Лимиты ПБС 2025 год")),
                    "obligation": parse_amount(item.get("Подтв. лимитов по БО 2025 год")),
                    "cash": parse_amount(item.get("Всего выбытий (бух.уч.)")),
                    "agreement": 0.0,
                    "contract": 0.0,
                    "payment": 0.0,
                    "buau": 0.0,
                }
            )


def load_agreements(records: list[dict]) -> None:
    folder = DATA_DIR / "2_Agreements"
    class_names = {
        "273": "МБТ",
        "278": "Иные цели БУ/АУ",
        "272": "Госзадание",
        "313": "ЮЛ/ИП/ФЛ",
    }
    for path in sorted(folder.glob("*.csv")):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                kcsr = display_code(row.get("kcsr_code"))
                recipient = (row.get("dd_recipient_caption") or row.get("dd_estimate_caption") or "").strip()
                records.append(
                    {
                        "source": "Соглашения",
                        "snapshot": agreement_snapshot(row, path.name),
                        "event_date": parse_date(row.get("close_date")),
                        "budget": (row.get("caption") or "").replace("!!! НЕ РАБОТАТЬ !!!", "").strip(),
                        "object_code": kcsr,
                        "object_code_norm": normalize_code(kcsr),
                        "object_name": recipient or kcsr,
                        "kfsr": display_code(row.get("kfsr_code")),
                        "kvr": display_code(row.get("kvr_code")),
                        "kosgu": display_code(row.get("kesr_code")),
                        "counterparty": recipient,
                        "document_number": (row.get("reg_number") or "").strip(),
                        "description": class_names.get(str(row.get("documentclass_id")), "Соглашение"),
                        "limit": 0.0,
                        "obligation": 0.0,
                        "cash": 0.0,
                        "agreement": parse_amount(row.get("amount_1year")),
                        "contract": 0.0,
                        "payment": 0.0,
                        "buau": 0.0,
                    }
                )


def load_state_task(records: list[dict]) -> None:
    folder = DATA_DIR / "3_StateTask"
    budget_lines: dict[str, list[dict[str, str]]] = defaultdict(list)
    lines_path = folder / "Бюджетные строки.csv"
    contracts_path = folder / "Контракты и договора.csv"
    payments_path = folder / "Платежки.csv"

    if lines_path.exists():
        with lines_path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                budget_lines[row["con_document_id"]].append(row)

    contracts: dict[str, dict[str, str]] = {}
    if contracts_path.exists():
        with contracts_path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                contracts[row["con_document_id"]] = row
                for line in budget_lines.get(row["con_document_id"], [{}]):
                    kcsr = display_code(line.get("kcsr_code"))
                    records.append(
                        {
                            "source": "ГЗ: контракты",
                            "snapshot": parse_date(row.get("con_date")),
                            "event_date": parse_date(row.get("con_date")),
                            "budget": "",
                            "object_code": kcsr,
                            "object_code_norm": normalize_code(kcsr),
                            "object_name": kcsr or row.get("con_number", "").strip(),
                            "kfsr": display_code(line.get("kfsr_code")),
                            "kvr": display_code(line.get("kvr_code")),
                            "kosgu": display_code(line.get("kesr_code")),
                            "counterparty": (row.get("zakazchik_key") or "").strip(),
                            "document_number": (row.get("con_number") or "").strip(),
                            "description": "Контракт/договор",
                            "limit": 0.0,
                            "obligation": 0.0,
                            "cash": 0.0,
                            "agreement": 0.0,
                            "contract": parse_amount(row.get("con_amount")),
                            "payment": 0.0,
                            "buau": 0.0,
                        }
                    )

    if payments_path.exists():
        with payments_path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                contract = contracts.get(row["con_document_id"], {})
                related_lines = budget_lines.get(row["con_document_id"], [{}])
                for line in related_lines:
                    kcsr = display_code(line.get("kcsr_code"))
                    records.append(
                        {
                            "source": "ГЗ: платежи",
                            "snapshot": parse_date(row.get("platezhka_paydate")),
                            "event_date": parse_date(row.get("platezhka_paydate")),
                            "budget": "",
                            "object_code": kcsr,
                            "object_code_norm": normalize_code(kcsr),
                            "object_name": kcsr or contract.get("con_number", "").strip(),
                            "kfsr": display_code(line.get("kfsr_code")),
                            "kvr": display_code(line.get("kvr_code")),
                            "kosgu": display_code(line.get("kesr_code")),
                            "counterparty": (contract.get("zakazchik_key") or "").strip(),
                            "document_number": (row.get("platezhka_num") or "").strip(),
                            "description": "Оплата по контракту",
                            "limit": 0.0,
                            "obligation": 0.0,
                            "cash": 0.0,
                            "agreement": 0.0,
                            "contract": 0.0,
                            "payment": parse_amount(row.get("platezhka_amount")),
                            "buau": 0.0,
                        }
                    )


def load_buau(records: list[dict]) -> None:
    folder = DATA_DIR / "4_BUAU_Export"
    for path in sorted(folder.glob("*.csv")):
        snapshot = buau_snapshot(path.name)
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle, delimiter=";"):
                kcsr = display_code(row.get("КЦСР"))
                organization = (row.get("Организация") or "").strip()
                records.append(
                    {
                        "source": "БУАУ",
                        "snapshot": snapshot,
                        "event_date": parse_date(row.get("Дата проводки")),
                        "budget": (row.get("Бюджет") or "").strip(),
                        "object_code": kcsr,
                        "object_code_norm": normalize_code(kcsr),
                        "object_name": organization or kcsr,
                        "kfsr": display_code(row.get("КФСР")),
                        "kvr": display_code(row.get("КВР")),
                        "kosgu": display_code(row.get("КОСГУ")),
                        "counterparty": organization,
                        "document_number": "",
                        "description": (row.get("Орган, предоставляющий субсидии") or "").strip(),
                        "limit": 0.0,
                        "obligation": 0.0,
                        "cash": 0.0,
                        "agreement": 0.0,
                        "contract": 0.0,
                        "payment": 0.0,
                        "buau": parse_amount(row.get("Выплаты с учетом возврата")),
                    }
                )


def apply_filters(records: list[dict], params: dict[str, list[str]]) -> list[dict]:
    text = params.get("q", [""])[0].strip().lower()
    code = normalize_code(params.get("code", [""])[0])
    budget = params.get("budget", [""])[0].strip().lower()
    source = params.get("source", [""])[0].strip()
    start = params.get("start", [""])[0].strip()
    end = params.get("end", [""])[0].strip()
    template = params.get("template", ["all"])[0].strip() or "all"

    result = []
    for record in records:
        haystack = " ".join(
            str(record.get(key, ""))
            for key in ("object_name", "object_code", "budget", "counterparty", "document_number", "description")
        ).lower()
        if text and text not in haystack:
            continue
        if code and code not in record.get("object_code_norm", ""):
            continue
        if budget and budget not in record.get("budget", "").lower():
            continue
        if source and record.get("source") != source:
            continue
        if not matches_template(record, template):
            continue
        date_value = record.get("snapshot") or record.get("event_date") or ""
        if start and date_value and date_value < start:
            continue
        if end and date_value and date_value > end:
            continue
        result.append(record)
    return result


def aggregate(records: list[dict], metrics: list[str] | None = None) -> dict:
    groups: dict[tuple[str, str], dict] = {}
    timeline: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    metric_keys = metrics or list(METRIC_KEYS)

    totals = {key: 0.0 for key in metric_keys}
    for record in records:
        object_key = record.get("object_code_norm") or record.get("object_name", "").lower()
        key = (object_key, record.get("budget") or "")
        row = groups.setdefault(
            key,
            {
                "object_code": record.get("object_code") or "",
                "object_name": record.get("object_name") or "",
                "budget": key[1],
                "sources": set(),
                **{metric: 0.0 for metric in METRIC_KEYS},
            },
        )
        if not row["object_code"] and record.get("object_code"):
            row["object_code"] = record["object_code"]
        if (
            record.get("source") == "РЧБ"
            and record.get("object_name")
            and len(record["object_name"]) > len(row["object_name"])
        ):
            row["object_name"] = record["object_name"]
        row["sources"].add(record["source"])
        point = record.get("snapshot") or record.get("event_date") or "unknown"
        for metric in METRIC_KEYS:
            value = float(record.get(metric) or 0.0)
            if metric in metric_keys:
                row[metric] += value
                totals[metric] += value
            if metric in metric_keys and point != "unknown":
                timeline[point][metric] += value

    rows = []
    for row in groups.values():
        row["sources"] = ", ".join(sorted(row["sources"]))
        row["total"] = sum(row[metric] for metric in metric_keys)
        rows.append(row)
    rows.sort(key=lambda item: item["total"], reverse=True)

    timeline_rows = []
    for date in sorted(timeline):
        point = {"date": date}
        point.update({metric: timeline[date].get(metric, 0.0) for metric in metric_keys})
        timeline_rows.append(point)

    return {
        "totals": totals,
        "rows": rows[:300],
        "details": records[:500],
        "timeline": timeline_rows,
        "count": len(records),
    }


def apply_aggregate_post_filter(result: dict, post_filter: str, metrics: list[str]) -> dict:
    if post_filter != "low_execution":
        return result
    rows = []
    for row in result.get("rows", []):
        plan = float(row.get("limit") or 0) + float(row.get("obligation") or 0)
        execution = float(row.get("cash") or 0) + float(row.get("payment") or 0) + float(row.get("buau") or 0)
        if plan > 0 and (execution == 0 or execution / plan < 0.25):
            rows.append(row)
    filtered = dict(result)
    filtered["rows"] = rows
    filtered["totals"] = {metric: sum(float(row.get(metric) or 0) for row in rows) for metric in metrics}
    filtered["count"] = len(rows)
    return filtered


def catalog_objects(records: list[dict], params: dict[str, list[str]]) -> list[dict]:
    query = params.get("q", [""])[0].strip().lower()
    template = params.get("template", ["all"])[0].strip() or "all"
    seen: set[tuple[str, str, str]] = set()
    objects = []
    for record in records:
        if not matches_template(record, template):
            continue
        haystack = " ".join(
            str(record.get(key, ""))
            for key in ("object_code", "object_name", "budget", "counterparty", "document_number")
        ).lower()
        if query and query not in haystack:
            continue
        item = (
            record.get("object_code", ""),
            record.get("object_name", ""),
            record.get("budget", ""),
        )
        if item in seen:
            continue
        seen.add(item)
        objects.append({"code": item[0], "name": item[1], "budget": item[2]})
        if len(objects) >= 200:
            break
    return objects


def load_rag_documents() -> list[dict]:
    documents = []
    if not RAG_DIR.exists():
        return documents
    for path in sorted(RAG_DIR.glob("*.md")):
        text = path.read_text(encoding="utf-8").strip()
        if text:
            documents.append({"source_file": relative_source(path), "title": path.stem, "content": text})
    return documents


def retrieve_rag_context(message: str, limit: int = 4) -> str:
    query_words = {word for word in re.findall(r"[0-9A-Za-zА-Яа-яЁё/]+", message.lower()) if len(word) > 1}
    scored = []
    for document in load_rag_documents():
        content = document["content"].lower()
        score = sum(1 for word in query_words if word in content)
        if score:
            scored.append((score, document))
    scored.sort(key=lambda item: item[0], reverse=True)
    selected = [document for _, document in scored[:limit]]
    if not selected and load_rag_documents():
        selected = load_rag_documents()[:1]
    return "\n\n".join(f"[{item['source_file']}]\n{item['content']}" for item in selected)


def clean_search_text(message: str) -> str:
    text = re.sub(r"\b(покажи|показать|найди|найти|сравни|сравнить|что|такое|где|есть|по|в|и|с|со)\b", " ", message, flags=re.I)
    text = re.sub(r"\b(скк|кик|окв|лимит\w*|касс\w*|исполн\w*|платеж\w*|оплат\w*|динамик\w*|измен\w*)\b", " ", text, flags=re.I)
    text = re.sub(r"\b(6105|978|970|2/3)\b", " ", text)
    return " ".join(text.split())


def assistant_rule_based(message: str, context: dict | None = None) -> dict:
    context = context or {}
    lower = message.lower()
    start, end = default_date_range()
    action = {
        "mode": context.get("mode") or "slice",
        "template": context.get("template") or "all",
        "q": "",
        "code": "",
        "budget": "",
        "source": "",
        "start": start,
        "end": end,
        "metrics": context.get("selected_metrics") or ["limit", "obligation", "cash"],
    }
    intent = "run_query"
    confidence = 0.62

    if "скк" in lower or "6105" in lower:
        action["template"] = "skk"
        confidence = 0.9
    elif "кик" in lower or "978" in lower:
        action["template"] = "kik"
        confidence = 0.88
    elif "2/3" in lower or "970" in lower:
        action["template"] = "two_thirds"
        confidence = 0.88
    elif "окв" in lower or "капитал" in lower or "капвлож" in lower:
        action["template"] = "okv"
        confidence = 0.86

    if any(word in lower for word in ("сравн", "измен", "динамик")):
        intent = "run_compare"
        action["mode"] = "compare"
        action["base"] = start
        action["target"] = end
        confidence = max(confidence, 0.82)

    if any(word in lower for word in ("касс", "исполн", "платеж", "оплат")):
        action["metrics"] = ["cash", "payment", "buau"]
    if any(word in lower for word in ("лимит", "план", "бо")):
        action["metrics"] = ["limit", "obligation"]
    if any(word in lower for word in ("проблем", "нет касс", "низк")):
        intent = "show_execution_problems"
        action["template"] = "all"
        action["metrics"] = ["limit", "cash", "payment", "buau"]

    if any(phrase in lower for phrase in ("что такое", "объясни", "расскажи")):
        intent = "explain_metric" if any(word in lower for word in ("бо", "касс", "лимит", "метрик")) else "explain_template"
        confidence = max(confidence, 0.75)

    search_text = clean_search_text(message)
    if intent in {"run_query", "run_compare", "find_object"} and search_text:
        action["q"] = search_text

    rag_context = retrieve_rag_context(message, limit=2)
    explanation = ""
    if intent.startswith("explain") and rag_context:
        explanation = " " + " ".join(rag_context.split())[:700]

    return {
        "mode": "rule_based",
        "intent": intent,
        "confidence": confidence,
        "message": f"Я понял запрос как {'сравнение' if intent == 'run_compare' else 'выборку'}: {TEMPLATES.get(action['template'], TEMPLATES['all'])['label']}.{explanation}",
        "action": action,
        "alternatives": [
            {"label": "Искать во всех данных", "action": {"mode": "slice", "template": "all", "q": search_text or message, "metrics": action["metrics"]}},
        ],
        "rag_context": rag_context,
    }


def validate_assistant_action(action: dict, fallback: dict) -> dict:
    result = dict(fallback)
    if not isinstance(action, dict):
        return result
    if action.get("mode") in {"slice", "compare"}:
        result["mode"] = action["mode"]
    if action.get("template") in TEMPLATES:
        result["template"] = action["template"]
    for key in ("q", "code", "budget", "source", "start", "end", "base", "target"):
        if key in action:
            result[key] = str(action.get(key) or "")[:200]
    metrics = action.get("metrics")
    if isinstance(metrics, list):
        filtered = [metric for metric in metrics if metric in METRIC_KEYS]
        if filtered:
            result["metrics"] = filtered
    return result


def assistant_llm(message: str, context: dict, rag_context: str) -> dict:
    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is not set")
    model = os.environ.get("GROQ_MODEL", "openai/gpt-oss-120b").strip() or "openai/gpt-oss-120b"
    start, end = default_date_range()
    system_prompt = (
        "Ты помощник конструктора бюджетных выборок. Ты не считаешь суммы сам. "
        "Верни только JSON без markdown. Допустимые intent: "
        + ", ".join(sorted(ASSISTANT_INTENTS))
        + ". Допустимые шаблоны: "
        + ", ".join(TEMPLATES)
        + ". Допустимые метрики: "
        + ", ".join(METRIC_KEYS)
        + ". JSON: {\"intent\":\"run_query\",\"confidence\":0.8,\"message\":\"...\",\"action\":{...},\"alternatives\":[]}."
    )
    user_payload = {
        "message": message,
        "context": context,
        "available_dates": {"start": start, "end": end, "all": STORE.meta.get("snapshots", [])},
        "templates": {code: item["description"] for code, item in TEMPLATES.items()},
        "metrics": METRICS,
        "rag_context": rag_context,
    }
    body = json.dumps(
        {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        },
        ensure_ascii=False,
    ).encode("utf-8")
    request = Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=body,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=15) as response:
        payload = json.loads(response.read().decode("utf-8"))
    content = payload["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    fallback = assistant_rule_based(message, context)
    intent = parsed.get("intent") if parsed.get("intent") in ASSISTANT_INTENTS else fallback["intent"]
    action = validate_assistant_action(parsed.get("action", {}), fallback["action"])
    return {
        "mode": "llm",
        "intent": intent,
        "confidence": float(parsed.get("confidence") or fallback["confidence"]),
        "message": str(parsed.get("message") or fallback["message"])[:1000],
        "action": action,
        "alternatives": parsed.get("alternatives") if isinstance(parsed.get("alternatives"), list) else fallback["alternatives"],
        "rag_context": rag_context,
    }


def assistant_response(message: str, context: dict | None = None) -> dict:
    context = context or {}
    fallback = assistant_rule_based(message, context)
    if os.environ.get("ASSISTANT_ENABLED", "auto").lower() == "false":
        return fallback
    if not os.environ.get("GROQ_API_KEY", "").strip():
        return fallback
    try:
        return assistant_llm(message, context, fallback.get("rag_context", ""))
    except Exception:
        return fallback


def trace_record(record_id: str) -> dict | None:
    for record in STORE.records:
        if record.get("id") == record_id:
            normalized = {key: value for key, value in record.items() if key not in {"raw"}}
            amount_fields = []
            for key, label in (
                ("limit", "Лимиты"),
                ("obligation", "БО"),
                ("cash", "Касса"),
                ("agreement", "Соглашения"),
                ("contract", "Договоры"),
                ("payment", "Оплаты"),
                ("buau", "БУ/АУ"),
            ):
                value = float(record.get(key) or 0)
                if value:
                    amount_fields.append({"label": label, "field": key, "value": value})
            return {
                "id": record.get("id"),
                "source": record.get("source"),
                "source_file": record.get("source_file", ""),
                "source_row": record.get("source_row", ""),
                "human_summary": {
                    "title": "Сумма из исходной строки",
                    "date": record.get("event_date") or record.get("snapshot") or "",
                    "object": record.get("object_name") or record.get("object_code") or "",
                    "document": record.get("document_number") or record.get("description") or "",
                    "amount_fields": amount_fields,
                },
                "raw": record.get("raw", {}),
                "normalized": normalized,
            }
    return None


def compare_periods(params: dict[str, list[str]]) -> dict:
    base = params.get("base", [""])[0].strip()
    target = params.get("target", [""])[0].strip()
    metrics = selected_metrics(params)
    filter_params = {key: value for key, value in params.items() if key not in {"start", "end", "base", "target"}}
    filtered = apply_filters(STORE.records, filter_params)
    base_rows = aggregate([record for record in filtered if record.get("snapshot") == base], metrics)["rows"]
    target_rows = aggregate([record for record in filtered if record.get("snapshot") == target], metrics)["rows"]
    by_key: dict[tuple[str, str], dict] = {}
    for label, rows in (("base", base_rows), ("target", target_rows)):
        for row in rows:
            key = (row.get("object_code") or row.get("object_name", ""), row.get("budget", ""))
            item = by_key.setdefault(
                key,
                {
                    "object_code": row.get("object_code", ""),
                    "object_name": row.get("object_name", ""),
                    "budget": row.get("budget", ""),
                    "sources": row.get("sources", ""),
                    "metrics": {metric: {"base": 0.0, "target": 0.0, "delta": 0.0, "delta_percent": None} for metric in metrics},
                },
            )
            if row.get("sources"):
                item["sources"] = row["sources"]
            for metric in metrics:
                item["metrics"][metric][label] = row.get(metric, 0.0)
    rows = []
    for item in by_key.values():
        total_delta = 0.0
        for metric in metrics:
            values = item["metrics"][metric]
            values["delta"] = values["target"] - values["base"]
            values["delta_percent"] = (values["delta"] / values["base"] * 100.0) if values["base"] else None
            total_delta += abs(values["delta"])
        item["total_delta"] = total_delta
        rows.append(item)
    rows.sort(key=lambda item: item["total_delta"], reverse=True)
    return {
        "base": base,
        "target": target,
        "metrics": metrics,
        "available_dates": STORE.meta["snapshots"],
        "rows": rows[:300],
    }


STORE = load_data()


class Handler(SimpleHTTPRequestHandler):
    def translate_path(self, path: str) -> str:
        parsed = urlparse(path)
        requested = parsed.path
        if requested == "/":
            return str(STATIC_DIR / "index.html")
        if requested.startswith("/static/"):
            return str(ROOT / requested.lstrip("/"))
        return str(STATIC_DIR / requested.lstrip("/"))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/meta":
            self.write_json(STORE.meta)
            return
        if parsed.path == "/api/query":
            params = parse_qs(parsed.query)
            filtered = apply_filters(STORE.records, params)
            metrics = selected_metrics(params)
            result = aggregate(filtered, metrics)
            post_filter = params.get("post_filter", [""])[0].strip()
            self.write_json(apply_aggregate_post_filter(result, post_filter, metrics))
            return
        if parsed.path == "/api/compare":
            self.write_json(compare_periods(parse_qs(parsed.query)))
            return
        if parsed.path == "/api/quality":
            self.write_json({"issues": QUALITY_ISSUES, "summary": quality_summary(), "load_stats": LOAD_STATS})
            return
        if parsed.path == "/api/trace":
            record_id = parse_qs(parsed.query).get("id", [""])[0]
            payload = trace_record(record_id)
            if payload is None:
                self.write_json({"error": "record_not_found"}, status=404)
            else:
                self.write_json(payload)
            return
        if parsed.path == "/api/catalog/dates":
            self.write_json(STORE.meta["snapshots"])
            return
        if parsed.path == "/api/catalog/sources":
            self.write_json(STORE.meta["sources"])
            return
        if parsed.path == "/api/catalog/budgets":
            self.write_json(STORE.meta["budgets"])
            return
        if parsed.path == "/api/catalog/templates":
            self.write_json(
                [{"code": code, "label": item["label"], "description": item["description"]} for code, item in TEMPLATES.items()]
            )
            return
        if parsed.path == "/api/catalog/metrics":
            self.write_json([{"code": code, "label": label} for code, label in METRICS.items()])
            return
        if parsed.path == "/api/catalog/quick-actions":
            self.write_json(quick_actions_payload())
            return
        if parsed.path == "/api/catalog/objects":
            self.write_json(catalog_objects(STORE.records, parse_qs(parsed.query)))
            return
        super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/assistant":
            try:
                length = int(self.headers.get("Content-Length", "0") or "0")
                payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                message = str(payload.get("message") or "").strip()
                context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
                if not message:
                    self.write_json({"error": "message_required"}, status=400)
                    return
                self.write_json(assistant_response(message, context))
            except json.JSONDecodeError:
                self.write_json({"error": "invalid_json"}, status=400)
            return
        self.write_json({"error": "not_found"}, status=404)

    def write_json(self, payload: object, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Loaded {STORE.meta['records']} records from {DATA_DIR}")
    print(f"Open http://127.0.0.1:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
