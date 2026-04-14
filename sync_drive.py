"""
sync_drive.py — Humand CS & Benchmark Repository Sync
Reads files from a Google Drive folder, parses filenames using the
Humand nomenclature, and writes data.json for the HTML dashboard.

Nomenclature: [TIPO]_[MODULO]_[CLIENTE]_[INDUSTRIA].[ext]
  TIPO    : BM (Benchmark) | CS (Customer Spotlight)
  MODULO  : See MODULE_MAP below
  CLIENTE : Company name (no spaces; use hyphens if needed)
  INDUSTRIA: See INDUSTRY_MAP below

Example: BM_COMUNICACIONES_NaranjaX_FINANZAS.pdf
"""

import json
import os
import sys
from datetime import datetime, timezone

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────

DRIVE_FOLDER_ID = "1F7w4bHTDJWT4phFIl80Z19iHApz9efWX"
OUTPUT_FILE = "data.json"

# Path to service account credentials JSON
# When running locally: place credentials.json next to this script
# When running via GitHub Actions: set the GOOGLE_CREDENTIALS env var
CREDENTIALS_FILE = "credentials.json"

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# ──────────────────────────────────────────────────────────────────────────────
# MODULE MAPPING  (filename code → display name)
# ──────────────────────────────────────────────────────────────────────────────

MODULE_MAP = {
    "MURO": "Muro",
    "NOTICIAS": "Noticias",
    "LIVESTREAMING": "Livestreaming",
    "MARKETPLACE": "Marketplace",
    "GRUPOS": "Grupos",
    "CHATS": "Chats",
    "ENCUESTAS": "Encuestas",
    "ACCESOS": "Accesos rápidos",
    "LIBRERIA": "Librería de conocimiento",
    "VACACIONES": "Vacaciones y permisos",
    "DOCUMENTOS": "Documentos personales",
    "ARCHIVOS": "Archivos",
    "ONBOARDING": "Onboarding",
    "CHATBOT": "Chatbot",
    "PX": "People Experience",
    "DESEMPENO": "Desempeño (Performance Review)",
    "OBJETIVOS": "Objetivos",
    "APRENDIZAJE": "Aprendizaje",
    "FORMULARIOS": "Formularios y trámites",
    "SERVICE": "Service Management",
    "HORARIO": "Control horario",
    "RECONOCIMIENTOS": "Reconocimientos",
    "EVENTOS": "Eventos",
    "ATS": "ATS",
}

# ──────────────────────────────────────────────────────────────────────────────
# INDUSTRY MAPPING  (filename code → display name)
# ──────────────────────────────────────────────────────────────────────────────

INDUSTRY_MAP = {
    "AGRICULTURE": "Agriculture",
    "PHARMA": "Pharmaceuticals",
    "UTILITIES": "Utilities (electricity, gas, water)",
    "OIL": "Oil & Energy",
    "TELCO": "Telecommunications",
    "CONSUMER": "Consumer Goods",
    "CONSTRUCTION": "Construction",
    "GOVERNMENT": "Government",
    "MANUFACTURING": "Manufacturing",
    "FINANZAS": "Financial Services",
    "SOFTWARE": "Software Companies & IT Services",
    "HEALTHCARE": "Healthcare",
    "SECURITY": "Security Services",
    "RETAIL": "Retail",
    "AUTOMOTIVE": "Automotive",
    "TRANSPORT": "Airlines, Transportation & Logistics",
}

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def get_drive_service():
    """Build and return an authenticated Drive API service."""
    # Try env var first (GitHub Actions), then fall back to file
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(creds_json)
            tmp_path = f.name
        credentials = service_account.Credentials.from_service_account_file(
            tmp_path, scopes=SCOPES
        )
        os.unlink(tmp_path)
    elif os.path.exists(CREDENTIALS_FILE):
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )
    else:
        print("ERROR: No credentials found.", file=sys.stderr)
        print(
            "  • Local: place credentials.json next to sync_drive.py", file=sys.stderr
        )
        print(
            "  • GitHub Actions: set GOOGLE_CREDENTIALS secret", file=sys.stderr
        )
        sys.exit(1)

    return build("drive", "v3", credentials=credentials)


def drive_url(file_id: str, mode: str = "view") -> str:
    """Return a Drive URL for viewing or downloading."""
    if mode == "download":
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"


def thumbnail_url(file_id: str) -> str:
    return f"https://drive.google.com/thumbnail?id={file_id}&sz=w400"


def parse_filename(name: str) -> dict | None:
    """
    Parse a filename following the Humand nomenclature.
    Returns a dict with parsed fields, or None if it doesn't match.

    Expected: [TIPO]_[MODULO]_[CLIENTE]_[INDUSTRIA]  (extension stripped)
    """
    # Strip extension
    stem = name.rsplit(".", 1)[0] if "." in name else name

    # Split on underscores — max 4 parts
    parts = stem.split("_", 3)
    if len(parts) < 4:
        return None

    tipo, modulo, cliente, industria = parts[0], parts[1], parts[2], parts[3]

    tipo_upper = tipo.upper()
    if tipo_upper not in ("BM", "CS"):
        return None

    return {
        "tipo": tipo_upper,
        "modulo_code": modulo.upper(),
        "modulo_display": MODULE_MAP.get(modulo.upper(), modulo.title()),
        "cliente": cliente.replace("-", " "),
        "industria_code": industria.upper(),
        "industria_display": INDUSTRY_MAP.get(industria.upper(), industria.title()),
    }


def list_folder(service, folder_id: str) -> list[dict]:
    """Recursively list all files in a Drive folder."""
    files = []
    page_token = None
    query = f"'{folder_id}' in parents and trashed = false"
    fields = "nextPageToken, files(id, name, mimeType, modifiedTime, size)"

    while True:
        response = (
            service.files()
            .list(
                q=query,
                fields=fields,
                pageToken=page_token,
                pageSize=200,
            )
            .execute()
        )
        for item in response.get("files", []):
            if item["mimeType"] == "application/vnd.google-apps.folder":
                # Recurse into sub-folders
                files.extend(list_folder(service, item["id"]))
            else:
                files.append(item)
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return files


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    print("Connecting to Google Drive…")
    service = get_drive_service()

    print(f"Listing folder {DRIVE_FOLDER_ID}…")
    raw_files = list_folder(service, DRIVE_FOLDER_ID)
    print(f"  Found {len(raw_files)} file(s)")

    benchmarks = []
    spotlights = []
    skipped = []

    for f in raw_files:
        parsed = parse_filename(f["name"])
        if not parsed:
            skipped.append(f["name"])
            continue

        ext = f["name"].rsplit(".", 1)[-1].lower() if "." in f["name"] else "unknown"

        entry = {
            "id": f["id"],
            "filename": f["name"],
            "display_name": f"{parsed['cliente']} — {parsed['modulo_display']}",
            "tipo": parsed["tipo"],
            "modulo_code": parsed["modulo_code"],
            "modulo_display": parsed["modulo_display"],
            "cliente": parsed["cliente"],
            "industria_code": parsed["industria_code"],
            "industria_display": parsed["industria_display"],
            "extension": ext,
            "modified": f.get("modifiedTime", ""),
            "view_url": drive_url(f["id"], "view"),
            "download_url": drive_url(f["id"], "download"),
            "thumbnail_url": thumbnail_url(f["id"]),
        }

        if parsed["tipo"] == "BM":
            benchmarks.append(entry)
        else:
            spotlights.append(entry)

    # Sort by client name
    benchmarks.sort(key=lambda x: x["cliente"].lower())
    spotlights.sort(key=lambda x: x["cliente"].lower())

    output = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_benchmarks": len(benchmarks),
        "total_spotlights": len(spotlights),
        "benchmarks": benchmarks,
        "spotlights": spotlights,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)

    print(f"\nDone!")
    print(f"  Benchmarks   : {len(benchmarks)}")
    print(f"  Spotlights   : {len(spotlights)}")
    if skipped:
        print(f"  Skipped (bad name format): {len(skipped)}")
        for s in skipped:
            print(f"    - {s}")
    print(f"  Output       : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
