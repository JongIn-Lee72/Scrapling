# Crawling Linkareer Activities and Syncing to Google Sheets

This tutorial shows a practical architecture using Scrapling's spider system to crawl the **Linkareer activities** listing and keep a Google Spreadsheet up-to-date.

!!! warning

    Always verify and follow Linkareer's Terms of Service and robots policy before crawling in production.

## Karpathy-style product principles (UX + AI cost)

If we optimize this pipeline with a strong product-engineering mindset:

1. **Deterministic first, AI second**: parse with selectors/regex before even thinking about LLM calls.
2. **Skip work aggressively**: if an item key already exists and you're not in full-refresh mode, don't fetch detail pages.
3. **Batch side effects**: group Google Sheets updates/appends to reduce API round-trips.
4. **Make defaults cheap**: keep AI enrichment off by default and turn it on only for rows that fail deterministic extraction.
5. **Single-command operation**: one script, env vars only, clear run mode (`FULL_REFRESH=0/1`).

## What you will build

- Crawl listing pages and activity detail pages with a `Spider`
- Route list pages through `FetcherSession` and escalate only detail pages to `AsyncStealthySession`
- Upsert rows into Google Sheets with a stable key
- Preserve `first_seen_at` and update `last_seen_at` on every re-crawl
- Detect content updates using `content_hash`
- Reduce traffic by skipping unchanged items by default

## Dependencies

```bash
pip install "scrapling[fetchers]" gspread google-auth
scrapling install
```

## Google Sheet setup

1. Create a Google Cloud project and enable **Google Sheets API**.
2. Create a service account and download its JSON key file.
3. Share your spreadsheet with the service account email (Editor role).
4. Set `GOOGLE_APPLICATION_CREDENTIALS` to the key path.

## Suggested worksheet schema

Use this exact column order in row 1:

```text
source, source_url, external_id, title, host_org, apply_start_at, apply_end_at, location,
status, content_hash, first_seen_at, last_seen_at
```

## End-to-end reference implementation

Save this as `linkareer_sync.py` and customize selectors/URLs for your target pages.

```python
import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable

import gspread
from google.oauth2.service_account import Credentials

from scrapling.fetchers import AsyncStealthySession, FetcherSession
from scrapling.spiders import Response, SessionManager, Spider


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_content_hash(payload: Dict[str, Any]) -> str:
    stable = "|".join(
        [
            payload.get("title", ""),
            payload.get("host_org", ""),
            payload.get("apply_start_at", ""),
            payload.get("apply_end_at", ""),
            payload.get("location", ""),
            payload.get("status", ""),
        ]
    )
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


@dataclass
class SheetConfig:
    spreadsheet_id: str
    worksheet_name: str


class GoogleSheetUpserter:
    COLUMNS = [
        "source",
        "source_url",
        "external_id",
        "title",
        "host_org",
        "apply_start_at",
        "apply_end_at",
        "location",
        "status",
        "content_hash",
        "first_seen_at",
        "last_seen_at",
    ]

    def __init__(self, cfg: SheetConfig) -> None:
        creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        creds = Credentials.from_service_account_file(
            creds_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        client = gspread.authorize(creds)
        self.ws = client.open_by_key(cfg.spreadsheet_id).worksheet(cfg.worksheet_name)

    def ensure_header(self) -> None:
        header = self.ws.row_values(1)
        if header != self.COLUMNS:
            self.ws.update("A1", [self.COLUMNS])

    def _fetch_existing_index(self) -> Dict[str, Dict[str, Any]]:
        records = self.ws.get_all_records()
        index: Dict[str, Dict[str, Any]] = {}
        for row_num, row in enumerate(records, start=2):
            key = row.get("external_id") or row.get("source_url")
            if key:
                index[key] = {
                    "row_num": row_num,
                    "first_seen_at": row.get("first_seen_at", ""),
                }
        return index

    def existing_keys(self) -> set[str]:
        return set(self._fetch_existing_index().keys())

    def upsert_many(self, items: Iterable[Dict[str, Any]]) -> None:
        index = self._fetch_existing_index()

        # NOTE: For high volume, replace per-row operations with batch_update payloads.
        for item in items:
            key = item.get("external_id") or item["source_url"]
            now = utcnow_iso()

            if key in index:
                existing = index[key]
                item["first_seen_at"] = existing.get("first_seen_at") or now
                item["last_seen_at"] = now
                row = [item.get(col, "") for col in self.COLUMNS]
                self.ws.update(f"A{existing['row_num']}", [row])
            else:
                item["first_seen_at"] = now
                item["last_seen_at"] = now
                row = [item.get(col, "") for col in self.COLUMNS]
                self.ws.append_row(row, value_input_option="RAW")


class LinkareerActivitiesSpider(Spider):
    name = "linkareer_activities"

    # Replace with actual listing URL and query params.
    start_urls = ["https://linkareer.com/list/extern"]
    allowed_domains = {"linkareer.com"}

    concurrent_requests = 3
    download_delay = 0.8
    max_blocked_retries = 3

    def __init__(self, known_keys: set[str], full_refresh: bool = False) -> None:
        self.known_keys = known_keys
        self.full_refresh = full_refresh
        super().__init__()

    def configure_sessions(self, manager: SessionManager) -> None:
        manager.add("http", FetcherSession(), default=True)
        manager.add(
            "stealth",
            AsyncStealthySession(
                headless=True,
                network_idle=True,
                disable_resources=True,
                solve_cloudflare=True,
            ),
            lazy=True,
        )

    async def parse(self, response: Response):
        # Replace selectors with real Linkareer selectors.
        for card in response.css(".activity-card"):
            detail_url = card.css("a::attr(href)").get()
            card_key = card.css("[data-activity-id]::attr(data-activity-id)").get("") or detail_url or ""

            # Cost optimization: skip existing rows unless full refresh is enabled.
            if (not self.full_refresh) and card_key and card_key in self.known_keys:
                continue

            if detail_url:
                yield response.follow(detail_url, callback=self.parse_detail, sid="stealth")

        next_page = response.css("a.next::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    async def parse_detail(self, response: Response):
        item = {
            "source": "linkareer",
            "source_url": str(response.url),
            "external_id": response.css("[data-activity-id]::attr(data-activity-id)").get("") or "",
            "title": response.css("h1::text").get(""),
            "host_org": response.css(".host-org::text").get(""),
            "apply_start_at": response.css(".apply-start::text").get(""),
            "apply_end_at": response.css(".apply-end::text").get(""),
            "location": response.css(".location::text").get(""),
            "status": response.css(".status::text").get("unknown"),
        }

        item["content_hash"] = make_content_hash(item)
        yield item


if __name__ == "__main__":
    sheet_cfg = SheetConfig(
        spreadsheet_id=os.environ["SHEET_ID"],
        worksheet_name=os.environ.get("WORKSHEET_NAME", "activities"),
    )
    full_refresh = os.environ.get("FULL_REFRESH", "0") == "1"

    upserter = GoogleSheetUpserter(sheet_cfg)
    upserter.ensure_header()

    spider = LinkareerActivitiesSpider(
        known_keys=upserter.existing_keys(),
        full_refresh=full_refresh,
    )
    result = spider.start()

    upserter.upsert_many(result.items)
    print(result.stats.to_dict())
```

## GUI mode (desktop)

If you prefer a general UX-oriented interface (instead of editing env vars each run), use:

```bash
python examples/linkareer_sync_gui.py
```

The GUI provides:

- Credentials path / Spreadsheet ID / Worksheet name inputs
- Credentials JSON file picker (`Browse...`)
- Full refresh toggle
- `Test Connection` button for quick Google Sheets API validation
- Start button + progress indicator
- Live log panel + completion/error dialogs
- Auto-save of recent settings to `.linkareer_gui_config.json`

## AI cost guardrail pattern (optional)

Keep AI enrichment explicitly gated:

```python
AI_ENRICHMENT = os.environ.get("AI_ENRICHMENT", "0") == "1"

if AI_ENRICHMENT and item_missing_required_fields:
    # call LLM only for failed deterministic parses
    ...
```

This pattern usually cuts token spend dramatically because most rows are handled by deterministic extraction.

## Production hardening checklist

- Implement custom `is_blocked()` if Linkareer returns anti-bot pages with HTTP 200.
- Use `crawldir=` to enable pause/resume for long runs.
- Batch Sheets writes (`batch_update`) at scale.
- Add retry/backoff for Sheets API `429` and transient `5xx`.
- Keep stealth requests minimal; route only necessary pages through stealth.
- Add observability: success rate, rows inserted/updated, skipped keys, run duration.

## Run periodically

```bash
python linkareer_sync.py
```

Typical env vars:

- `GOOGLE_APPLICATION_CREDENTIALS`
- `SHEET_ID`
- `WORKSHEET_NAME`
- `FULL_REFRESH` (`0` or `1`)
- `AI_ENRICHMENT` (`0` or `1`)
