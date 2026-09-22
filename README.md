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
2. WHAT IS NEEDED FROM ANGEL ONE (SmartAPI)
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
    This script automatically SPLITS the requested date range into
    chunks that respect these limits, and stitches the results together,
    so one can just enter any start/end date and it figures out the rest.

    FOR LARGE DOWNLOADS (2-3+ years of data):
    - Each chunk is saved to a temporary folder AS IT DOWNLOADS
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
