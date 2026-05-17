import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

try:
    from . import utils
except ImportError:
    import utils

METADATA_FIELDS = [
    "TournamentID",
    "Name",
    "Type",
    "Status",
    "Level",
    "SWRMultiplier",
    "Country",
    "City",
    "Address",
    "Date",
    "EndDate",
    "StartTime",
    "RegistrationStart",
    "RegistrationEnd",
    "Participants",
    "MaxParticipants",
    "Email",
    "PhoneNumber",
    "DetailURL",
    "ExtraMetadataJson",
    "ScrapedAt",
]

STAGE_FIELDS = [
    "TournamentID",
    "StageID",
    "StageSequence",
    "StageURL",
]

INFO_FIELD_MAP = {
    "Tournament type": "Type",
    "Status": "Status",
    "Level of tournament": "Level",
    "Country": "Country",
    "City": "City",
    "Address": "Address",
    "Date of the tournament": "Date",
    "Date of the end of the tournament": "EndDate",
    "Time of the start": "StartTime",
    "Beginning of the registration": "RegistrationStart",
    "Finishing of the registration": "RegistrationEnd",
    "Participants": "Participants",
    "Max. number of participants": "MaxParticipants",
    "E-mail": "Email",
    "Phone number": "PhoneNumber",
}

DATE_FIELDS = {"Date", "EndDate"}
DATETIME_FIELDS = {"RegistrationStart", "RegistrationEnd"}
INT_FIELDS = {"TournamentID", "Level", "Participants", "MaxParticipants"}
FLOAT_FIELDS = {"SWRMultiplier"}

def extract_tournament_id(url: str) -> str:
    match = re.search(r"/tournament/id/(\d+)/?", url)
    if match:
        return match.group(1)
    return url.rstrip("/").split("/")[-1]

def _first_number(value: str) -> str:
    match = re.search(r"\d+", value or "")
    return match.group(0) if match else ""

def _first_float(value: str) -> str:
    match = re.search(r"\d+(?:[.,]\d+)?", value or "")
    return match.group(0).replace(",", ".") if match else ""

def _format_date(value: str) -> str:
    value = (value or "").strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return value

def _format_datetime(value: str) -> str:
    value = (value or "").strip()
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y %H.%M", "%d.%m.%y %H:%M", "%d.%m.%y %H.%M"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d %H:%M")
        except ValueError:
            continue
    return value

def _format_time(value: str) -> str:
    value = (value or "").strip()
    match = re.search(r"(\d{1,2})[:.](\d{2})", value)
    if not match:
        return value
    return f"{int(match.group(1)):02d}:{match.group(2)}"

def normalize_metadata_value(field: str, value: str) -> str:
    value = (value or "").strip()
    if field in DATE_FIELDS:
        return _format_date(value)
    if field in DATETIME_FIELDS:
        return _format_datetime(value)
    if field == "StartTime":
        return _format_time(value)
    if field in INT_FIELDS:
        return _first_number(value)
    if field in FLOAT_FIELDS:
        return _first_float(value)
    return value

def parse_tournament_metadata(
    soup,
    detail_url: str,
    fallback_id: Optional[str] = None,
    fallback_name: Optional[str] = None,
) -> Dict[str, str]:
    info = utils.parse_info_table(soup)
    header = soup.select_one("h1#header")
    name = header.get_text(" ", strip=True) if header else (fallback_name or "")
    tournament_id = fallback_id or extract_tournament_id(detail_url)

    row = {field: "" for field in METADATA_FIELDS}
    row.update({
        "TournamentID": normalize_metadata_value("TournamentID", tournament_id),
        "Name": name,
        "DetailURL": detail_url,
        "ScrapedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })

    extras = {}
    for label, raw_value in info.items():
        field = INFO_FIELD_MAP.get(label)
        if not field and "SWR" in label:
            field = "SWRMultiplier"
        if field:
            row[field] = normalize_metadata_value(field, raw_value)
        else:
            extras[label] = raw_value

    row["ExtraMetadataJson"] = json.dumps(extras, ensure_ascii=False, sort_keys=True) if extras else ""
    return row

def catalog_row_from_metadata(row: Dict[str, str]) -> Dict[str, str]:
    return {
        "ID": row.get("TournamentID", ""),
        "Name": row.get("Name", ""),
        "Type": row.get("Type", ""),
    }

def parse_tournament_stages(soup, tournament_id: str, base_url: str) -> List[Dict[str, str]]:
    stages = []
    current_stage_sequence = ""

    for row in soup.select("table.stages-table tr"):
        seq_cell = row.select_one("td.stage-gr")
        if seq_cell:
            current_stage_sequence = seq_cell.get_text(strip=True)

        sched_link = row.select_one('a:-soup-contains("Schedule and results")')
        if not sched_link:
            continue

        href = sched_link.get("href", "")
        match = re.search(r"/tournament/stage/(\d+)/matches/?", href)
        if not match:
            continue

        stage_id = match.group(1)
        stages.append({
            "TournamentID": str(tournament_id),
            "StageID": stage_id,
            "StageSequence": current_stage_sequence,
            "StageURL": f"{base_url}{href}?print",
        })

    return stages

def read_metadata_csv(filename: Path) -> List[Dict[str, str]]:
    if not filename.exists():
        return []
    with filename.open("r", encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        return [{field: row.get(field, "") for field in METADATA_FIELDS} for row in reader]

def write_metadata_csv(filename: Path, rows: Iterable[Dict[str, str]]) -> None:
    filename.parent.mkdir(parents=True, exist_ok=True)
    with filename.open("w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=METADATA_FIELDS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in METADATA_FIELDS})

def upsert_metadata_csv(filename: Path, rows: Iterable[Dict[str, str]]) -> None:
    incoming = [row for row in rows if row.get("TournamentID")]
    if not incoming:
        return

    existing = read_metadata_csv(filename)
    incoming_by_id = {row["TournamentID"]: row for row in incoming}
    merged = []

    for row in existing:
        replacement = incoming_by_id.pop(row.get("TournamentID", ""), None)
        merged.append(replacement or row)

    merged.extend(incoming_by_id.values())
    write_metadata_csv(filename, merged)

def read_stage_csv(filename: Path) -> List[Dict[str, str]]:
    if not filename.exists():
        return []
    with filename.open("r", encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        return [{field: row.get(field, "") for field in STAGE_FIELDS} for row in reader]

def write_stage_csv(filename: Path, rows: Iterable[Dict[str, str]]) -> None:
    filename.parent.mkdir(parents=True, exist_ok=True)
    with filename.open("w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=STAGE_FIELDS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in STAGE_FIELDS})

def upsert_stage_csv(filename: Path, rows: Iterable[Dict[str, str]]) -> None:
    incoming = [row for row in rows if row.get("TournamentID") and row.get("StageID")]
    if not incoming:
        return

    existing = read_stage_csv(filename)
    incoming_tournament_ids = {row["TournamentID"] for row in incoming}
    merged = [
        row for row in existing
        if row.get("TournamentID") not in incoming_tournament_ids
    ]
    merged.extend(incoming)
    write_stage_csv(filename, merged)
