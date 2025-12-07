from __future__ import annotations
# at top with imports
import calendar
from datetime import datetime

import re
from dataclasses import dataclass, asdict
from datetime import datetime
from io import BytesIO
from typing import List, Tuple

import pandas as pd
import pdfplumber
from dateutil.parser import parse as dtparse


# =========================
# Constants & Patterns
# =========================

KNOWN_CATEGORIES = [
    "Personal and Household Expenses",
    "Professional and Financial Services",
    "Retail and Grocery",
    "Transportation",
    "Hotel, Entertainment and Recreation",
    "Restaurants",
    "Health and Education",
    "Foreign Currency Transactions",
    "Other Transactions",
]

PROVINCES = {"ON","QC","BC","AB","MB","SK","NB","NS","NL","PE","YT","NT","NU"}

# Cities (longest-first). Add as you see new ones.
CITY_PATTERNS = [
    # ON
    "NIAGARA FALLS","RICHMOND HILL","STONEY CREEK",
    "MISSISSAUGA","ETOBICOKE","WOODBRIDGE","DESERONTO",
    "BRAMPTON","OTTAWA","NEPEAN","TORONTO","MARKHAM","KANATA","ORLEANS",
    # QC
    "SAINT GABRIEL","MONTREAL","WAKEFIELD",
    # AB
    "CALGARY",
    # NS
    "HALIFAX",
    # NB
    "FREDERICTON",
]
CITY_PATTERNS_SORTED = sorted(CITY_PATTERNS, key=len, reverse=True)
CITY_PATTERNS_NOSPACE = [c.replace(" ", "") for c in CITY_PATTERNS_SORTED]

PAYMENTS_START = "Your payments"
PAYMENTS_TOTAL_PREFIX = "Total payments $"

CHARGES_START = "Your new charges and credits"
TOTAL_FOR_PREFIX = "Total for"
CARD_NUMBER_PREFIX = "Card number"

TRANSACTIONS_FROM_RE = re.compile(r"Transactions from .*? (\d{4})")

MONTH_3 = r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
DATE_RE = rf"{MONTH_3}\s+\d{{1,2}}"   # 1–2 digit day

AMOUNT_RE = r"-?\d[\d,]*\.?\d{0,2}"   # 830 | 146.9 | 1,234.56 | -12.34

ROW_RE = re.compile(
    rf"^(?P<tdate>{DATE_RE})\s+(?P<pdate>{DATE_RE})\s+(?P<body>.+?)\s+(?P<amount>{AMOUNT_RE})$"
)


# =========================
# Data Models
# =========================

@dataclass
class PaymentRow:
    trans_date: datetime
    post_date: datetime
    description: str
    amount: float
    source: str = "CIBC"


@dataclass
class TransactionRow:
    trans_date: datetime
    post_date: datetime
    description: str
    category: str
    amount: float
    location: str = ""   # "City PROV" or "" when no city
    city: str = ""       # "City" or ""
    province: str = ""   # "PROV" or ""
    source: str = "CIBC"


# =========================
# Helpers
# =========================

def _extract_year(pages_text: List[str]) -> int:
    for t in pages_text:
        m = TRANSACTIONS_FROM_RE.search(t or "")
        if m:
            return int(m.group(1))
    return datetime.now().year


def _normalize_date(dstr: str, year: int) -> datetime:
    """
    Parse 'Mon DD' with provided year. If the day is out of range for the
    month (e.g., Feb 29 on a non-leap year), clamp to the month's last day.
    """
    try:
        return dtparse(f"{dstr} {year}")
    except ValueError as e:
        # Typical error: "day is out of range for month: Feb 29 2025"
        if "day is out of range for month" in str(e):
            parts = str(dstr).split()
            if len(parts) >= 2:
                mon_str, day_str = parts[0], parts[1]
                # convert month abbrev to month number
                try:
                    month = datetime.strptime(mon_str, "%b").month
                    day = int(day_str)
                    last_day = calendar.monthrange(year, month)[1]
                    return datetime(year, month, min(day, last_day))
                except Exception:
                    pass
        # If it’s some other parsing issue, bubble it up
        raise


def _find_category_and_desc(body: str) -> Tuple[str, str]:
    body_norm = " ".join(str(body).split())
    for cat in sorted(KNOWN_CATEGORIES, key=len, reverse=True):
        if body_norm.endswith(cat):
            desc = body_norm[: -len(cat)].rstrip()
            return cat, desc
    return "", body_norm


def _normalize_city(s: str) -> str:
    s = " ".join(str(s).split())
    return " ".join(w.capitalize() for w in s.split())
# put near the top, alongside imports
def _letters_only(s: str) -> str:
    return re.sub(r"[^A-Z]", "", s.upper())


def _detect_province_suffix(U: str) -> tuple[re.Match | None, str, str]:
    """
    Return (match, province, mode)
    mode:
      - 'space'   => province preceded by whitespace
      - 'hyphen'  => province preceded by hyphen (e.g., MID-HNS)
      - 'domain'  => URL/brand/phone tail; if province present, treat as province-only
      - 'gluedcity'=> known city immediately followed by province with no separator
    """
    # 1) Safe: whitespace before province
    m = re.search(r"\s(ON|QC|BC|AB|MB|SK|NB|NS|NL|PE|YT|NT|NU)\s*$", U)
    if m:
        return m, m.group(1), "space"

    # 2) Hyphen-glued: ...-HNS, ...-EON, etc.
    m = re.search(r"-(ON|QC|BC|AB|MB|SK|NB|NS|NL|PE|YT|NT|NU)\s*$", U)
    if m:
        return m, m.group(1), "hyphen"

    # 3) Domain/brand tails (URL/phone/vendor hints) → province-only if present
    domain_hints = ("WWW", "HTTP", "HTTPS", ".COM", ".CA", "/", "G.CO", "GOOGLE", "AMAZON", "UBER")
    if any(h in U for h in domain_hints):
        m = re.search(r"(ON|QC|BC|AB|MB|SK|NB|NS|NL|PE|YT|NT|NU)\s*$", U)
        if m:
            return m, m.group(1), "domain"

    # 4) NEW: Glued city + province (no separator): e.g., NIAGARAFALLS + ON → 'NIAGARA FALLSON'
    # Normalize by stripping non-letters for the comparison.
    for prov in ("ON","QC","BC","AB","MB","SK","NB","NS","NL","PE","YT","NT","NU"):
        if U.endswith(prov):
            before = U[: -len(prov)].rstrip()
            before_letters = _letters_only(before)
            for city_ns in CITY_PATTERNS_NOSPACE:
                if before_letters.endswith(city_ns):
                    # fabricate a minimal match-like object for callers
                    class _FakeMatch:
                        def __init__(self, start, end): self._s, self._e = start, end
                        def start(self): return self._s
                        def end(self): return self._e
                        def group(self, *_): return prov
                    return _FakeMatch(len(before), len(U)), prov, "gluedcity"

    return None, "", ""


def _extract_location(desc: str) -> tuple[str, str, str]:
    """
    Robust (location, city, province) extractor.
    - Avoid false provinces inside words (e.g., ADJUSTMENT).
    - Handle domain/URL/phone tails (UBER.COM/CA/EON, AMAZON.CAON, g.co/helppay#NS) -> province only.
    - De-glue city names (ONLINEOTTAWA, RESTAURBRAMPTON) -> proper city.
    - Prefer real cities if present (e.g., HALIFAX in IC* INSTACART ... MID-HNS).
    """
    U = str(desc).upper().strip()

    # Province detection (with mode)
    m, prov, mode = _detect_province_suffix(U)
    if not m:
        return ("", "", "")

    # Text before the province token
    tail_raw = U[:m.start()].rstrip()

    # De-glue frequent prefixes that stick to city names
    tail_raw = re.sub(r"(HTTPSWWW|HTTPS|HTTP|WWW|G\.?CO/HELPPAY|GCO/HELPPAY|ONLINE)(?=[A-Z])", " ", tail_raw)
    tail_raw = tail_raw.replace("UBERCOM", "UBER COM").replace("AMAZON.CA", "AMAZON CA")

    # Alpha-only for city scanning
    tail_alpha = re.sub(r"[^A-Z\s]", " ", tail_raw)
    tail_alpha = " ".join(tail_alpha.split())
    toks = tail_alpha.split()

    # Noise/brand tokens that are not cities
    STOP = {
        "STORE","SUPERCENTER","AMAZON","AMAZONCA","WWW","HTTPS","HTTP","UBER","UBERCOM",
        "GOOGLE","YOUTUBE","G","CO","HELPPAY","AIRBNB","WESTJET","INC","LTD","COMP","COM","ONLINE",
        "SUPERCENTEROTTAWA","SUPERCENTERNEPEAN","COMPMONTREAL","INCTORONTO","CKANATA",
        "AMAZONCAON","UBERON","HTTPSWWW",
    }
    JOINERS = {"CREEK", "FALLS", "HILL"}  # prefer two-word cities

    def pick_city_from_tokens() -> str:
        # A) Try two-word joiners
        if len(toks) >= 2 and toks[-1] in JOINERS and re.match(r"^[A-Z]+$", toks[-2]):
            return f"{toks[-2]} {toks[-1]}"
        # B) Last token, but repair glued cases like RESTAURBRAMPTON
        if toks:
            last_tok = toks[-1]
            if last_tok not in STOP and re.match(r"^[A-Z]+$", last_tok):
                # Too-short "city" near domain or phone tails -> ignore (e.g., 'E', 'M', 'CA')
                if len(last_tok) < 3:
                    return ""
                # If last_tok contains a known city suffix, pick that (handle glue)
                for pat, pat_ns in zip(CITY_PATTERNS_SORTED, CITY_PATTERNS_NOSPACE):
                    if last_tok.endswith(pat_ns) or pat in last_tok:
                        return pat
                return last_tok
        return ""

    # Prefer real city unless mode implies domain tail
    cand_city = "" if mode == "domain" else pick_city_from_tokens()

    # If still empty or noisy, scan anywhere in alpha tail (fix glued or earlier city)
    if not cand_city or cand_city in STOP:
        for pat in CITY_PATTERNS_SORTED:
            if pat in tail_alpha:
                cand_city = pat
                break

    # Special: if we have HALIFAX somewhere and province NS, prefer Halifax
    if not cand_city and prov == "NS" and "HALIFAX" in tail_alpha:
        cand_city = "HALIFAX"

    # Final tidy
    if cand_city and cand_city not in STOP:
        city_norm = _normalize_city(cand_city)
        return (f"{city_norm} {prov}", city_norm, prov)

    # Province only (no reliable city)
    return ("", "", prov)


def _is_noise(line: str) -> bool:
    s = (line or "").strip()
    if not s:
        return True
    if s.startswith(CARD_NUMBER_PREFIX):
        return True
    if s.startswith(TOTAL_FOR_PREFIX):
        return True
    if re.match(r"^Page \d+ of \d+$", s):
        return True
    if re.match(r"^\*\d{7,}\*$", s):     # *0502530000*
        return True
    if re.match(r"^-?\d{3}-\d{6,}$", s): # -188-036281
        return True
    return False


def _pages_text_from_path(pdf_path: str) -> List[str]:
    with pdfplumber.open(pdf_path) as pdf:
        return [p.extract_text() or "" for p in pdf.pages]


def _pages_text_from_filelike(fp: BytesIO) -> List[str]:
    try:
        fp.seek(0)
    except Exception:
        pass
    with pdfplumber.open(fp) as pdf:
        return [p.extract_text() or "" for p in pdf.pages]


def _parse_pages_text(pages_text: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    year = _extract_year(pages_text)

    in_payments = False
    in_charges = False
    payments: List[PaymentRow] = []
    txns: List[TransactionRow] = []

    for page_text in pages_text:
        for raw_line in (page_text or "").splitlines():
            line = " ".join(raw_line.split())

            # Section switching
            if line.startswith(PAYMENTS_START):
                in_payments, in_charges = True, False
                continue
            if line.startswith(CHARGES_START):
                in_charges, in_payments = True, False
                continue
            if in_payments and line.startswith(PAYMENTS_TOTAL_PREFIX):
                in_payments = False
                continue

            if _is_noise(line):
                continue

            m = ROW_RE.match(line)
            if not m:
                continue

            tdate = _normalize_date(m.group("tdate"), year)
            pdate = _normalize_date(m.group("pdate"), year)
            body = m.group("body")
            amt_str = m.group("amount").replace(",", "")
            amount = float(amt_str)

            if in_payments:
                payments.append(
                    PaymentRow(
                        trans_date=tdate,
                        post_date=pdate,
                        description=body,
                        amount=amount,
                    )
                )
            elif in_charges:
                cat, desc = _find_category_and_desc(body)
                loc, city, prov = _extract_location(desc)
                txns.append(
                    TransactionRow(
                        trans_date=tdate,
                        post_date=pdate,
                        description=desc,
                        category=cat,
                        amount=amount,
                        location=loc,
                        city=city,
                        province=prov,
                    )
                )

    payments_df = pd.DataFrame([asdict(p) for p in payments]) if payments else pd.DataFrame(
        columns=["trans_date", "post_date", "description", "amount", "source"]
    )
    txns_df = pd.DataFrame([asdict(t) for t in txns]) if txns else pd.DataFrame(
        columns=["trans_date", "post_date", "description", "category", "amount", "location", "city", "province", "source"]
    )

    # Dtypes
    for col in ("trans_date", "post_date"):
        if col in payments_df.columns:
            payments_df[col] = pd.to_datetime(payments_df[col], errors="coerce")
        if col in txns_df.columns:
            txns_df[col] = pd.to_datetime(txns_df[col], errors="coerce")

    if "amount" in payments_df.columns:
        payments_df["amount"] = pd.to_numeric(payments_df["amount"], errors="coerce").astype(float)
    if "amount" in txns_df.columns:
        txns_df["amount"] = pd.to_numeric(txns_df["amount"], errors="coerce").astype(float)

    # Clean text
    for df in (payments_df, txns_df):
        if "description" in df.columns:
            df["description"] = (
                df["description"].astype(str)
                .str.replace(r"\s{2,}", " ", regex=True)
                .str.strip()
            )
        if "source" in df.columns:
            df["source"] = df["source"].fillna("CIBC")

    return payments_df, txns_df


# =========================
# Public APIs
# =========================

def extract_cibc_payments_and_transactions(pdf_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    pages_text = _pages_text_from_path(pdf_path)
    return _parse_pages_text(pages_text)


def extract_cibc_from_filelike(fp: BytesIO) -> Tuple[pd.DataFrame, pd.DataFrame]:
    pages_text = _pages_text_from_filelike(fp)
    return _parse_pages_text(pages_text)