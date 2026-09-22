"""
Angel One (SmartAPI) Historical Data Downloader — GUI
======================================================

Download OHLC historical candle data from Angel One's SmartAPI by entering
a stock symbol, an exchange, an interval, and a start/end date.

------------------------------------------------------------------
1. INSTALL DEPENDENCIES
------------------------------------------------------------------
    pip install smartapi-python pyotp pandas requests

------------------------------------------------------------------
2. WHAT YOU NEED FROM ANGEL ONE (SmartAPI)
------------------------------------------------------------------
    - API Key          : create an app at https://smartapi.angelbroking.com/
    - Client ID        : your Angel One login / client code
    - PIN              : your 4-digit trading PIN
    - TOTP Secret Key  : the secret shown when you enable "External TOTP"
                          under your Angel One profile -> "Enable TOTP".
                          (This is NOT your regular login OTP — it's a
                          one-time secret string used to auto-generate
                          the 2FA code, similar to Google Authenticator.)

    None of these are stored anywhere by this script except in memory
    while it runs.

------------------------------------------------------------------
3. NOTES ON THE API'S OWN LIMITS
------------------------------------------------------------------
    Angel One restricts how much data you can request per call, and it
    varies by interval (roughly):
        ONE_MINUTE / THREE_MINUTE / FIVE_MINUTE  -> ~30 days per request
        TEN_MINUTE / FIFTEEN_MINUTE / THIRTY_MINUTE -> ~60 days per request
        ONE_HOUR                                  -> ~100 days per request
        ONE_DAY                                   -> ~365 days (2000 candles) per request
    This script automatically SPLITS your requested date range into
    chunks that respect these limits, and stitches the results together,
    so you can just enter any start/end date and it figures out the rest.

    FOR LARGE DOWNLOADS (2-3+ years of data):
    - Each chunk is saved to a temporary folder AS IT DOWNLOADS, so if the
      connection drops, Angel One rate-limits you, or you close the app
      partway through, you don't lose progress.
    - If a chunk hits a rate limit, it's automatically retried with a
      growing wait time (up to 5 attempts) before moving on.
    - If you re-run the same request (same symbol/exchange/interval/dates),
      already-downloaded chunks are detected and skipped instead of
      re-fetched.
    - Once every chunk is done, all of them are merged, de-duplicated,
      sorted by date, and written out as a single final CSV file.

------------------------------------------------------------------
4. RUN
------------------------------------------------------------------
    python angelone_data_downloader.py
"""

import glob
import hashlib
import json
import os
import tempfile
import threading
import time
import tkinter as tk
from datetime import datetime, timedelta
from tkinter import ttk, messagebox, filedialog

import pandas as pd
import requests

try:
    import pyotp
except ImportError:
    pyotp = None

try:
    from SmartApi import SmartConnect
except ImportError:
    try:
        from smartapi import SmartConnect  # older package name
    except ImportError:
        SmartConnect = None


INSTRUMENT_MASTER_URL = (
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
)
INSTRUMENT_CACHE_FILE = os.path.join(os.path.expanduser("~"), ".angelone_instrument_master.json")
INSTRUMENT_CACHE_MAX_AGE_HOURS = 24

INTERVALS = [
    "ONE_MINUTE",
    "THREE_MINUTE",
    "FIVE_MINUTE",
    "TEN_MINUTE",
    "FIFTEEN_MINUTE",
    "THIRTY_MINUTE",
    "ONE_HOUR",
    "ONE_DAY",
]

# Max number of days worth of data to request per single API call, per interval.
# Kept slightly under Angel One's documented limits as a safety margin.
CHUNK_DAYS = {
    "ONE_MINUTE": 25,
    "THREE_MINUTE": 25,
    "FIVE_MINUTE": 25,
    "TEN_MINUTE": 55,
    "FIFTEEN_MINUTE": 55,
    "THIRTY_MINUTE": 55,
    "ONE_HOUR": 90,
    "ONE_DAY": 350,
}

EXCHANGES = ["NSE", "BSE", "NFO", "MCX", "CDS"]

# Retry behaviour when Angel One's API rate-limits or blips a request.
MAX_RETRIES = 5
RETRY_BASE_WAIT_SECONDS = 3  # doubles each retry: 3s, 6s, 12s, 24s, 48s
REQUEST_DELAY_SECONDS = 1.0  # starting polite delay between successful calls
MAX_REQUEST_DELAY_SECONDS = 20.0  # ceiling for the adaptive slow-down

CHUNK_CACHE_ROOT = os.path.join(tempfile.gettempdir(), "angelone_downloader_chunks")


def get_job_cache_dir(symbol, exchange, interval, start_dt, end_dt):
    """
    A stable folder (per symbol/exchange/interval/date-range) where downloaded
    chunks are cached, so a re-run can resume instead of re-fetching everything.
    """
    key = f"{symbol}|{exchange}|{interval}|{start_dt.date()}|{end_dt.date()}"
    digest = hashlib.md5(key.encode()).hexdigest()[:12]
    job_dir = os.path.join(CHUNK_CACHE_ROOT, digest)
    os.makedirs(job_dir, exist_ok=True)
    return job_dir


def chunk_cache_path(job_dir, chunk_index):
    return os.path.join(job_dir, f"chunk_{chunk_index:04d}.csv")


def load_instrument_master(force_refresh=False, log=print):
    """Download (or load cached) Angel One instrument master list."""
    use_cache = False
    if not force_refresh and os.path.exists(INSTRUMENT_CACHE_FILE):
        age_hours = (time.time() - os.path.getmtime(INSTRUMENT_CACHE_FILE)) / 3600.0
        if age_hours < INSTRUMENT_CACHE_MAX_AGE_HOURS:
            use_cache = True

    if use_cache:
        log("Loading cached instrument master...")
        with open(INSTRUMENT_CACHE_FILE, "r") as f:
            return json.load(f)

    log("Downloading instrument master from Angel One (this can take a moment)...")
    resp = requests.get(INSTRUMENT_MASTER_URL, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    with open(INSTRUMENT_CACHE_FILE, "w") as f:
        json.dump(data, f)
    log("Instrument master downloaded and cached.")
    return data


def find_symbol_token(instruments, symbol, exchange):
    """
    Find the symbol token for a given trading symbol + exchange.
    Tries an exact 'symbol' match first (e.g. 'SBIN-EQ' for cash-market NSE),
    then falls back to a looser match on 'name'.
    """
    symbol = symbol.strip().upper()
    exchange = exchange.strip().upper()

    candidates = [c for c in instruments if c.get("exch_seg", "").upper() == exchange]

    # 1) exact symbol match, e.g. user typed "SBIN-EQ"
    for c in candidates:
        if c.get("symbol", "").upper() == symbol:
            return c

    # 2) exact symbol match after appending "-EQ" (common for NSE/BSE cash equity)
    if exchange in ("NSE", "BSE"):
        for c in candidates:
            if c.get("symbol", "").upper() == f"{symbol}-EQ":
                return c

    # 3) exact match on 'name' field with EQ series
    for c in candidates:
        if c.get("name", "").upper() == symbol and c.get("symbol", "").upper().endswith("-EQ"):
            return c

    # 4) exact match on 'name' field, any series
    for c in candidates:
        if c.get("name", "").upper() == symbol:
            return c

    # 5) loosest fallback: symbol starts with the given text
    for c in candidates:
        if c.get("symbol", "").upper().startswith(symbol):
            return c

    return None


def daterange_chunks(start_dt, end_dt, chunk_days):
    """Yield (chunk_start, chunk_end) datetime pairs covering start_dt..end_dt."""
    current = start_dt
    while current < end_dt:
        chunk_end = min(current + timedelta(days=chunk_days), end_dt)
        yield current, chunk_end
        current = chunk_end


class DownloaderApp:
    def __init__(self, root):
        self.root = root
        root.title("Angel One Historical Data Downloader")
        root.geometry("560x640")
        root.resizable(False, False)

        self.smart_api = None
        self.instruments_cache = None
        self.request_delay = REQUEST_DELAY_SECONDS

        pad = {"padx": 8, "pady": 4}

        # ---------------- Credentials ----------------
        cred_frame = ttk.LabelFrame(root, text="Angel One SmartAPI Credentials")
        cred_frame.pack(fill="x", **pad)

        self.api_key_var = tk.StringVar()
        self.client_id_var = tk.StringVar()
        self.pin_var = tk.StringVar()
        self.totp_secret_var = tk.StringVar()

        self._add_labeled_entry(cred_frame, "API Key:", self.api_key_var, row=0, show="*")
        self._add_labeled_entry(cred_frame, "Client ID:", self.client_id_var, row=1)
        self._add_labeled_entry(cred_frame, "PIN:", self.pin_var, row=2, show="*")
        self._add_labeled_entry(cred_frame, "TOTP Secret:", self.totp_secret_var, row=3, show="*")

        self.login_status_var = tk.StringVar(value="Not connected")
        ttk.Button(cred_frame, text="Login", command=self.on_login).grid(
            row=4, column=0, sticky="w", **pad
        )
        ttk.Label(cred_frame, textvariable=self.login_status_var, foreground="blue").grid(
            row=4, column=1, sticky="w", **pad
        )

        # ---------------- Request params ----------------
        req_frame = ttk.LabelFrame(root, text="Data Request")
        req_frame.pack(fill="x", **pad)

        self.symbol_var = tk.StringVar()
        self.exchange_var = tk.StringVar(value="NSE")
        self.interval_var = tk.StringVar(value="ONE_DAY")
        self.start_date_var = tk.StringVar(value=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        self.end_date_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))

        ttk.Label(req_frame, text="Stock symbol (e.g. SBIN, RELIANCE, TCS):").grid(
            row=0, column=0, sticky="w", **pad
        )
        ttk.Entry(req_frame, textvariable=self.symbol_var, width=25).grid(
            row=0, column=1, sticky="w", **pad
        )

        ttk.Label(req_frame, text="Exchange:").grid(row=1, column=0, sticky="w", **pad)
        ttk.Combobox(
            req_frame, textvariable=self.exchange_var, values=EXCHANGES, width=22, state="readonly"
        ).grid(row=1, column=1, sticky="w", **pad)

        ttk.Label(req_frame, text="Interval:").grid(row=2, column=0, sticky="w", **pad)
        ttk.Combobox(
            req_frame, textvariable=self.interval_var, values=INTERVALS, width=22, state="readonly"
        ).grid(row=2, column=1, sticky="w", **pad)

        ttk.Label(req_frame, text="Start date (YYYY-MM-DD):").grid(row=3, column=0, sticky="w", **pad)
        ttk.Entry(req_frame, textvariable=self.start_date_var, width=25).grid(
            row=3, column=1, sticky="w", **pad
        )

        ttk.Label(req_frame, text="End date (YYYY-MM-DD):").grid(row=4, column=0, sticky="w", **pad)
        ttk.Entry(req_frame, textvariable=self.end_date_var, width=25).grid(
            row=4, column=1, sticky="w", **pad
        )

        ttk.Button(req_frame, text="Download Data", command=self.on_download).grid(
            row=5, column=0, columnspan=2, pady=10
        )

        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(
            req_frame, orient="horizontal", mode="determinate", variable=self.progress_var, length=460
        )
        self.progress_bar.grid(row=6, column=0, columnspan=2, padx=8, pady=(0, 4))

        self.progress_label_var = tk.StringVar(value="")
        ttk.Label(req_frame, textvariable=self.progress_label_var).grid(
            row=7, column=0, columnspan=2, sticky="w", padx=8, pady=(0, 8)
        )

        # ---------------- Log output ----------------
        log_frame = ttk.LabelFrame(root, text="Log")
        log_frame.pack(fill="both", expand=True, **pad)

        self.log_text = tk.Text(log_frame, height=18, wrap="word", state="disabled")
        self.log_text.pack(fill="both", expand=True, padx=4, pady=4)

    def _add_labeled_entry(self, parent, label, var, row, show=None):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", padx=8, pady=4)
        entry = ttk.Entry(parent, textvariable=var, width=35, show=show if show else "")
        entry.grid(row=row, column=1, sticky="w", padx=8, pady=4)
        return entry

    def log(self, message):
        def append():
            self.log_text.configure(state="normal")
            self.log_text.insert("end", f"{message}\n")
            self.log_text.see("end")
            self.log_text.configure(state="disabled")

        self.root.after(0, append)

    # ---------------- Login ----------------
    def on_login(self):
        if SmartConnect is None:
            messagebox.showerror(
                "Missing dependency",
                "smartapi-python is not installed.\nRun: pip install smartapi-python pyotp",
            )
            return
        if pyotp is None:
            messagebox.showerror("Missing dependency", "Run: pip install pyotp")
            return

        api_key = self.api_key_var.get().strip()
        client_id = self.client_id_var.get().strip()
        pin = self.pin_var.get().strip()
        totp_secret = self.totp_secret_var.get().strip()

        if not all([api_key, client_id, pin, totp_secret]):
            messagebox.showwarning("Missing info", "Please fill in all credential fields.")
            return

        threading.Thread(
            target=self._login_worker, args=(api_key, client_id, pin, totp_secret), daemon=True
        ).start()

    def _login_worker(self, api_key, client_id, pin, totp_secret):
        try:
            self.log("Logging in to Angel One SmartAPI...")
            totp = pyotp.TOTP(totp_secret).now()
            smart_api = SmartConnect(api_key=api_key)
            session = smart_api.generateSession(client_id, pin, totp)

            if not session or not session.get("status", False):
                msg = session.get("message", "Unknown error") if session else "No response"
                self.log(f"Login failed: {msg}")
                self.login_status_var.set("Login failed")
                return

            self.smart_api = smart_api
            self.login_status_var.set("Connected ✓")
            self.log("Login successful.")
        except Exception as e:
            self.log(f"Login error: {e}")
            self.login_status_var.set("Login failed")

    # ---------------- Download ----------------
    def on_download(self):
        if self.smart_api is None:
            messagebox.showwarning("Not connected", "Please log in first.")
            return

        symbol = self.symbol_var.get().strip()
        exchange = self.exchange_var.get().strip()
        interval = self.interval_var.get().strip()
        start_str = self.start_date_var.get().strip()
        end_str = self.end_date_var.get().strip()

        if not symbol:
            messagebox.showwarning("Missing info", "Please enter a stock symbol.")
            return

        try:
            start_dt = datetime.strptime(start_str, "%Y-%m-%d")
            end_dt = datetime.strptime(end_str, "%Y-%m-%d") + timedelta(hours=23, minutes=59)
        except ValueError:
            messagebox.showerror("Invalid date", "Dates must be in YYYY-MM-DD format.")
            return

        if start_dt >= end_dt:
            messagebox.showerror("Invalid range", "Start date must be before end date.")
            return

        threading.Thread(
            target=self._download_worker,
            args=(symbol, exchange, interval, start_dt, end_dt),
            daemon=True,
        ).start()

    def _set_progress(self, done, total, extra=""):
        pct = (done / total * 100) if total else 0

        def update():
            self.progress_var.set(pct)
            self.progress_label_var.set(f"{done}/{total} chunks ({pct:.0f}%) {extra}")

        self.root.after(0, update)

    def _fetch_chunk_with_retry(self, exchange, token, interval, c_start, c_end, chunk_num, total_chunks):
        """
        Fetch one chunk of candle data, retrying with exponential backoff if
        Angel One rate-limits the request or returns a transient/malformed error.
        Returns a list of candle rows (possibly empty) on success, or None on
        hard failure after exhausting retries. Never raises.
        """
        params = {
            "exchange": exchange,
            "symboltoken": str(token),
            "interval": interval,
            "fromdate": c_start.strftime("%Y-%m-%d %H:%M"),
            "todate": c_end.strftime("%Y-%m-%d %H:%M"),
        }

        for attempt in range(1, MAX_RETRIES + 1):
            last_error = "Unknown error"
            is_rate_limit = False

            try:
                response = self.smart_api.getCandleData(params)

                if isinstance(response, dict) and response.get("status", False):
                    return response.get("data", [])

                # Response came back but wasn't a successful dict — could be a
                # dict with status False, or (as Angel One sometimes does) a
                # raw error string/bytes when it can't produce JSON at all.
                if isinstance(response, dict):
                    last_error = str(response.get("message", "Unknown error"))
                else:
                    last_error = str(response)

            except Exception as e:
                # Covers network errors, JSON decode errors, or any other
                # exception raised inside the SmartAPI client library.
                last_error = str(e)

            is_rate_limit = any(
                phrase in last_error.lower()
                for phrase in ("rate", "exceed", "access denied", "too many")
            )

            if is_rate_limit:
                # Slow down ALL future requests in this job, not just retries
                # of this one chunk — the server told us we're going too fast.
                self.request_delay = min(self.request_delay * 2, MAX_REQUEST_DELAY_SECONDS)
                self.log(
                    f"    [{chunk_num}/{total_chunks}] rate limit hit — slowing down to "
                    f"{self.request_delay:.1f}s between requests for the rest of this job."
                )

            if attempt < MAX_RETRIES:
                wait = RETRY_BASE_WAIT_SECONDS * (2 ** (attempt - 1))
                reason = "rate limited" if is_rate_limit else f"error ({last_error})"
                self.log(
                    f"    [{chunk_num}/{total_chunks}] {reason}, retrying in {wait}s "
                    f"(attempt {attempt}/{MAX_RETRIES})..."
                )
                time.sleep(wait)
            else:
                self.log(
                    f"    [{chunk_num}/{total_chunks}] giving up after {MAX_RETRIES} attempts: {last_error}"
                )

        return None  # signals hard failure for this chunk

    def _download_worker(self, symbol, exchange, interval, start_dt, end_dt):
        try:
            if self.instruments_cache is None:
                self.instruments_cache = load_instrument_master(log=self.log)

            self.log(f"Looking up symbol token for '{symbol}' on {exchange}...")
            instrument = find_symbol_token(self.instruments_cache, symbol, exchange)
            if instrument is None:
                self.log(f"Could not find a matching instrument for '{symbol}' on {exchange}.")
                self.log("Tip: try the exact trading symbol, e.g. 'SBIN-EQ' or just 'SBIN'.")
                return

            token = instrument["token"]
            resolved_symbol = instrument.get("symbol", symbol)
            self.log(f"Found: {resolved_symbol} (token={token})")

            chunk_days = CHUNK_DAYS.get(interval, 30)
            chunks = list(daterange_chunks(start_dt, end_dt, chunk_days))
            total_chunks = len(chunks)
            total_days = (end_dt - start_dt).days
            self.log(
                f"Date range spans ~{total_days} days -> split into {total_chunks} "
                f"chunk(s) for interval {interval}."
            )

            job_dir = get_job_cache_dir(symbol, exchange, interval, start_dt, end_dt)
            self.log(f"Chunk cache: {job_dir}")

            self.request_delay = REQUEST_DELAY_SECONDS  # adaptive; grows if we get rate-limited
            failed_chunks = []
            self._set_progress(0, total_chunks)

            for i, (c_start, c_end) in enumerate(chunks, start=1):
                cache_path = chunk_cache_path(job_dir, i)

                if os.path.exists(cache_path):
                    self.log(f"  [{i}/{total_chunks}] already downloaded, skipping (resumed).")
                    self._set_progress(i, total_chunks)
                    continue

                self.log(
                    f"  [{i}/{total_chunks}] {c_start.strftime('%Y-%m-%d %H:%M')} -> "
                    f"{c_end.strftime('%Y-%m-%d %H:%M')}"
                )

                candles = self._fetch_chunk_with_retry(
                    exchange, token, interval, c_start, c_end, i, total_chunks
                )

                if candles is None:
                    failed_chunks.append((i, c_start, c_end))
                    self._set_progress(i, total_chunks, extra="(1 failed)")
                    continue

                # Save this chunk to disk immediately so progress is never lost.
                chunk_df = pd.DataFrame(
                    candles, columns=["datetime", "open", "high", "low", "close", "volume"]
                )
                chunk_df.to_csv(cache_path, index=False)

                self._set_progress(i, total_chunks)
                time.sleep(self.request_delay)

            if failed_chunks:
                self.log(
                    f"{len(failed_chunks)} chunk(s) could not be downloaded after retries. "
                    f"Re-run the same request later to fetch just those (already-downloaded "
                    f"chunks will be skipped)."
                )

            # ---- Merge every cached chunk for this job into one final file ----
            chunk_files = sorted(glob.glob(os.path.join(job_dir, "chunk_*.csv")))
            if not chunk_files:
                self.log("No data was downloaded for the given parameters.")
                return

            self.log(f"Merging {len(chunk_files)} downloaded chunk(s)...")
            df = pd.concat((pd.read_csv(f) for f in chunk_files), ignore_index=True)
            df.drop_duplicates(subset="datetime", inplace=True)
            df.sort_values("datetime", inplace=True)
            df.reset_index(drop=True, inplace=True)

            self.log(f"Total candles after merge & de-dup: {len(df)}")

            default_name = f"{resolved_symbol}_{exchange}_{interval}_{start_dt.date()}_{end_dt.date()}.csv"
            save_path = filedialog.asksaveasfilename(
                initialfile=default_name,
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
            )
            if not save_path:
                self.log("Save cancelled. Merged data was not written to disk (chunk cache kept).")
                return

            df.to_csv(save_path, index=False)
            self.log(f"Saved final merged file to: {save_path}")

            if not failed_chunks:
                # Everything succeeded and was saved -> safe to clear the chunk cache.
                try:
                    for f in chunk_files:
                        os.remove(f)
                    os.rmdir(job_dir)
                except OSError:
                    pass

            summary = f"Downloaded {len(df)} rows to:\n{save_path}"
            if failed_chunks:
                summary += f"\n\n({len(failed_chunks)} chunk(s) failed — re-run to fill the gaps.)"
            self.root.after(0, lambda: messagebox.showinfo("Done", summary))

        except Exception as e:
            self.log(f"Error: {e}")


def main():
    root = tk.Tk()
    app = DownloaderApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()