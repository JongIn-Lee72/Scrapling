"""Simple desktop GUI for Linkareer -> Google Sheets sync.

This example intentionally keeps selectors/URL placeholders so users can adapt
it to the current Linkareer page structure.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from queue import Empty, Queue
from tkinter import BOTH, END, LEFT, RIGHT, TOP, VERTICAL, W, Button, Checkbutton, Entry, Frame, IntVar, Label, StringVar, Tk
from tkinter import filedialog
from tkinter import messagebox
from tkinter.scrolledtext import ScrolledText
from tkinter.ttk import Progressbar
from typing import Any

from google.oauth2.service_account import Credentials
import gspread

from scrapling.fetchers import AsyncStealthySession, FetcherSession
from scrapling.spiders import Response, SessionManager, Spider

CONFIG_PATH = ".linkareer_gui_config.json"


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_content_hash(payload: dict[str, Any]) -> str:
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

    def __init__(self, cfg: SheetConfig, credentials_path: str) -> None:
        creds = Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        client = gspread.authorize(creds)
        self.ws = client.open_by_key(cfg.spreadsheet_id).worksheet(cfg.worksheet_name)

    def ensure_header(self) -> None:
        header = self.ws.row_values(1)
        if header != self.COLUMNS:
            self.ws.update("A1", [self.COLUMNS])

    def _fetch_existing_index(self) -> dict[str, dict[str, Any]]:
        records = self.ws.get_all_records()
        index: dict[str, dict[str, Any]] = {}
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

    def upsert_many(self, items: list[dict[str, Any]]) -> tuple[int, int]:
        index = self._fetch_existing_index()
        inserted = 0
        updated = 0

        for item in items:
            key = item.get("external_id") or item["source_url"]
            now = utcnow_iso()
            if key in index:
                existing = index[key]
                item["first_seen_at"] = existing.get("first_seen_at") or now
                item["last_seen_at"] = now
                row = [item.get(col, "") for col in self.COLUMNS]
                self.ws.update(f"A{existing['row_num']}", [row])
                updated += 1
            else:
                item["first_seen_at"] = now
                item["last_seen_at"] = now
                row = [item.get(col, "") for col in self.COLUMNS]
                self.ws.append_row(row, value_input_option="RAW")
                inserted += 1
        return inserted, updated


class LinkareerActivitiesSpider(Spider):
    name = "linkareer_activities_gui"
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
        # Replace with real selectors from Linkareer.
        for card in response.css(".activity-card"):
            detail_url = card.css("a::attr(href)").get()
            card_key = card.css("[data-activity-id]::attr(data-activity-id)").get("") or detail_url or ""

            if (not self.full_refresh) and card_key and card_key in self.known_keys:
                continue

            if detail_url:
                yield response.follow(detail_url, callback=self.parse_detail, sid="stealth")

        next_page = response.css("a.next::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    async def parse_detail(self, response: Response):
        # Replace with real selectors from Linkareer.
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


class LinkareerSyncGUI:
    def __init__(self, root: Tk) -> None:
        self.root = root
        self.root.title("Linkareer → Google Sheets Sync")
        self.root.geometry("900x620")

        saved = self._load_saved_config()
        self.credentials_var = StringVar(
            value=saved.get("credentials_path", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""))
        )
        self.sheet_id_var = StringVar(value=saved.get("sheet_id", os.environ.get("SHEET_ID", "")))
        self.worksheet_var = StringVar(value=saved.get("worksheet_name", os.environ.get("WORKSHEET_NAME", "activities")))
        self.full_refresh_var = IntVar(
            value=saved.get("full_refresh", 1 if os.environ.get("FULL_REFRESH", "0") == "1" else 0)
        )

        self.log_queue: Queue[str] = Queue()
        self.is_running = False

        self._build_layout()
        self._poll_logs()

    @staticmethod
    def _load_saved_config() -> dict[str, Any]:
        if not os.path.exists(CONFIG_PATH):
            return {}
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception:
            return {}
        return {}

    def _save_config(self) -> None:
        data = {
            "credentials_path": self.credentials_var.get().strip(),
            "sheet_id": self.sheet_id_var.get().strip(),
            "worksheet_name": self.worksheet_var.get().strip(),
            "full_refresh": int(self.full_refresh_var.get()),
        }
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _build_layout(self) -> None:
        top_frame = Frame(self.root)
        top_frame.pack(side=TOP, fill=BOTH, padx=12, pady=12)

        self._labeled_input(top_frame, "Credentials JSON Path", self.credentials_var, 0)
        Button(top_frame, text="Browse...", command=self.pick_credentials_file, width=10).grid(
            row=0, column=2, sticky=W, padx=(8, 0)
        )
        self._labeled_input(top_frame, "Spreadsheet ID", self.sheet_id_var, 1)
        self._labeled_input(top_frame, "Worksheet Name", self.worksheet_var, 2)

        options = Frame(top_frame)
        options.grid(row=3, column=0, columnspan=2, sticky=W, pady=(8, 8))
        Checkbutton(options, text="Full refresh", variable=self.full_refresh_var).pack(side=LEFT)
        Label(
            top_frame,
            text="Tip: Share your sheet with the service account email before testing connection.",
        ).grid(row=4, column=0, columnspan=3, sticky=W, pady=(2, 8))

        actions = Frame(self.root)
        actions.pack(side=TOP, fill=BOTH, padx=12)

        self.run_btn = Button(actions, text="Start Sync", command=self.start_sync, width=16)
        self.run_btn.pack(side=LEFT)
        Button(actions, text="Test Connection", command=self.test_connection, width=16).pack(side=LEFT, padx=(8, 0))

        self.progress = Progressbar(actions, orient=VERTICAL, mode="indeterminate", length=120)
        self.progress.pack(side=RIGHT)

        self.log_text = ScrolledText(self.root, height=26)
        self.log_text.pack(side=TOP, fill=BOTH, expand=True, padx=12, pady=12)
        self.log_text.insert(END, "Ready. Fill fields and click 'Start Sync'.\n")

    @staticmethod
    def _labeled_input(parent: Frame, label: str, var: StringVar, row: int) -> None:
        Label(parent, text=label).grid(row=row, column=0, sticky=W, padx=(0, 10), pady=4)
        Entry(parent, textvariable=var, width=90).grid(row=row, column=1, sticky=W, pady=4)

    def _log(self, message: str) -> None:
        self.log_queue.put(message)

    def _poll_logs(self) -> None:
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log_text.insert(END, f"{msg}\n")
                self.log_text.see(END)
        except Empty:
            pass
        self.root.after(120, self._poll_logs)

    def _validate_inputs(self) -> bool:
        if not self.credentials_var.get().strip():
            messagebox.showerror("Missing input", "Credentials JSON Path is required.")
            return False
        if not os.path.exists(self.credentials_var.get().strip()):
            messagebox.showerror("Invalid input", "Credentials JSON file path does not exist.")
            return False
        if not self.sheet_id_var.get().strip():
            messagebox.showerror("Missing input", "Spreadsheet ID is required.")
            return False
        if not self.worksheet_var.get().strip():
            messagebox.showerror("Missing input", "Worksheet Name is required.")
            return False
        return True

    def start_sync(self) -> None:
        if self.is_running:
            return
        if not self._validate_inputs():
            return

        self._save_config()
        self.is_running = True
        self.run_btn.config(state="disabled")
        self.progress.start(8)
        self._log("Starting sync job...")

        worker = threading.Thread(target=self._run_sync, daemon=True)
        worker.start()

    def _finish(self) -> None:
        self.is_running = False
        self.progress.stop()
        self.run_btn.config(state="normal")

    def _run_sync(self) -> None:
        try:
            credentials_path = self.credentials_var.get().strip()
            cfg = SheetConfig(
                spreadsheet_id=self.sheet_id_var.get().strip(),
                worksheet_name=self.worksheet_var.get().strip(),
            )
            full_refresh = bool(self.full_refresh_var.get())

            upserter = GoogleSheetUpserter(cfg, credentials_path)
            upserter.ensure_header()
            self._log("Connected to Google Sheets successfully.")

            spider = LinkareerActivitiesSpider(
                known_keys=upserter.existing_keys(),
                full_refresh=full_refresh,
            )
            self._log("Starting crawler...")
            result = spider.start()

            inserted, updated = upserter.upsert_many(result.items)
            self._log(f"Sync complete. items={len(result.items)}, inserted={inserted}, updated={updated}")
            self._log(f"Stats: {result.stats.to_dict()}")

            messagebox.showinfo("Done", f"Sync completed\nInserted: {inserted}\nUpdated: {updated}")
        except Exception as exc:  # noqa: BLE001
            self._log(f"Error: {exc}")
            messagebox.showerror("Sync failed", str(exc))
        finally:
            self.root.after(0, self._finish)

    def pick_credentials_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Select Google service account JSON",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if path:
            self.credentials_var.set(path)

    def test_connection(self) -> None:
        if not self._validate_inputs():
            return
        try:
            cfg = SheetConfig(
                spreadsheet_id=self.sheet_id_var.get().strip(),
                worksheet_name=self.worksheet_var.get().strip(),
            )
            upserter = GoogleSheetUpserter(cfg, self.credentials_var.get().strip())
            _ = upserter.ws.row_values(1)
            self._save_config()
            messagebox.showinfo("Connection OK", "Google Sheets API connection succeeded.")
            self._log("Connection test passed.")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Connection Failed", str(exc))
            self._log(f"Connection test failed: {exc}")


def main() -> None:
    root = Tk()
    app = LinkareerSyncGUI(root)
    _ = app
    root.mainloop()


if __name__ == "__main__":
    main()
