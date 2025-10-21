"""
B2B outreach pipeline (Norway)

Hva denne skripten gjør:
1) Henter bedrifter fra Enhetsregisteret (BRREG) basert på kommune og NACE-kode(r)
2) Forsøker å finne nettside for hver bedrift og ekstrahere e‑post fra kontakt-sider
3) Lagrer alt i SQLite og CSV (deduplisert og validert)
4) Sende personlige e‑poster via SendGrid (eller SMTP) med mal (Jinja2)
5) Rate‑limit og logging, samt enkel «unsubscribe»-håndtering

Avhengigheter:
  pip install requests beautifulsoup4 lxml tldextract jinja2 python-dotenv sendgrid

Miljøvariabler:
  SENDGRID_API_KEY=...  (eller sett SMTP_* variabler hvis du bruker SMTP)
  OUTREACH_FROM_NAME=...
  OUTREACH_FROM_EMAIL=...
  OUTREACH_REPLY_TO=...
  OUTREACH_UNSUBSCRIBE_URL=https://dittdomene.no/unsubscribe?email={{email}}

NB:
- Følg BRREG sine vilkår for bruk av API. Ikke overskrid fornuftige rater.
- Følg nettsteders robots.txt. Ikke crawle aggressivt, og respekter ToS.
- Send bare relevante henvendelser til bedriftsadresser i tråd med markedsføringsloven/GDPR.
"""
from __future__ import annotations
import csv
import os
import re
import sqlite3
import time
import random
import json
from dataclasses import dataclass, asdict
from typing import Iterable, List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import tldextract
from jinja2 import Template

# ---------------------------
# Konfigurasjon
# ---------------------------
BRREG_BASE = "https://data.brreg.no/enhetsregisteret/api/enheter"
USER_AGENT = "LocalCateringOutreachBot/1.0 (+contact: post@dittdomene.no)"
DB_PATH = "outreach.db"
CSV_PATH = "outreach_prospects.csv"
LOG_PATH = "outreach.log"

# Filtrering – juster for din kommune og bransjer du vil treffe
DEFAULT_MUNICIPALITY_NUMBERS = ["0301"]  # Oslo=0301, Bergen=4601, Stavanger=1103, Trondheim=5001, Drammen=3005, etc.
TARGET_NACE_PREFIXES = ["56"]  # 56 = Serveringsvirksomhet (restauranter, kantiner, catering), juster for målgrupper

# Crawling-innstillinger
REQUEST_TIMEOUT = 15
CRAWL_DELAY = (1.0, 3.0)  # min, max sekunder mellom requests
MAX_PAGES_PER_SITE = 3
EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

# Send-innstillinger
SEND_RATE_SECONDS = (10, 25)  # delay mellom utsendelser
BATCH_LIMIT = 80              # maks antall e‑poster per kjøring

# ---------------------------
# Datastrukturer
# ---------------------------
@dataclass
class Company:
    orgnr: str
    name: str
    municipality: str
    nace: str
    website: Optional[str] = None
    email: Optional[str] = None
    source: str = "brreg"

# ---------------------------
# Verktøy
# ---------------------------

def log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
    print(msg)


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS companies (
          orgnr TEXT PRIMARY KEY,
          name TEXT,
          municipality TEXT,
          nace TEXT,
          website TEXT,
          email TEXT,
          source TEXT,
          last_seen INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sent (
          email TEXT PRIMARY KEY,
          company_orgnr TEXT,
          sent_at INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS unsubscribed (
          email TEXT PRIMARY KEY,
          unsubscribed_at INTEGER
        )
        """
    )
    conn.commit()
    return conn


# ---------------------------
# 1) Hent bedrifter fra BRREG
# ---------------------------

def fetch_from_brreg(municipality_numbers: List[str], nace_prefixes: List[str], max_pages: Optional[int] = None, verbose: bool = True) -> List[Company]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    companies: List[Company] = []

    for muni in municipality_numbers:
        page = 0
        seen_page_signatures = set()
        while True:
            if max_pages is not None and page >= max_pages:
                if verbose:
                    log(f"Reached max_pages={max_pages} for {muni}")
                break
            params = {
                "kommunenummer": muni,
                "size": 100,
                "page": page,
                "sort": "navn,asc",
            }
            if verbose:
                log(f"BRREG GET page={page} muni={muni} params={params}")
            try:
                r = requests.get(BRREG_BASE, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                log(f"Error fetching BRREG page {page} for {muni}: {e}")
                break

            embedded = data.get("_embedded", {})
            enheter = embedded.get("enheter", [])
            # Safeguard: stop if the page repeats (some APIs can echo last page)
            page_sig = tuple(sorted(e.get("organisasjonsnummer") for e in enheter))
            if page_sig in seen_page_signatures:
                log("Detected repeating page; stopping pagination to prevent infinite loop")
                break
            seen_page_signatures.add(page_sig)

            if not enheter:
                if verbose:
                    log("No more entities; stopping")
                break

            for e in enheter:
                orgnr = str(e.get("organisasjonsnummer", ""))
                name = e.get("navn", "").strip()
                municipality = (e.get("forretningsadresse") or {}).get("kommunenummer", "")
                nace = ((e.get("naeringskode1") or {}).get("kode", ""))
                website = (e.get("hjemmeside") or "") or None
                if nace_prefixes and nace and not any(nace.startswith(pref) for pref in nace_prefixes):
                    continue
                companies.append(Company(orgnr=orgnr, name=name, municipality=municipality, nace=nace, website=website))
            # Try to use API's own page info if available
            total_pages = None
            try:
                page_info = data.get("page", {})
                total_pages = page_info.get("totalPages")
            except Exception:
                total_pages = None

            page += 1
            if total_pages is not None and page >= total_pages:
                if verbose:
                    log(f"Reached last page per API (totalPages={total_pages}) for {muni}")
                break
            time.sleep(random.uniform(*CRAWL_DELAY))
    return companies


# ---------------------------
# 2) Finn e‑post fra nettsted
# ---------------------------

def normalize_url(url: str) -> Optional[str]:
    if not url:
        return None
    url = url.strip()
    if not url:
        return None
    if not url.startswith("http"):
        url = "http://" + url
    # Enkel normalisering / sanitering
    return url.rstrip("/")


def fetch(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            return None
        return r.text
    except Exception:
        return None


def extract_emails_from_html(html: str) -> List[str]:
    if not html:
        return []
    emails = set(m.group(0).lower() for m in EMAIL_REGEX.finditer(html))
    # Filtrer åpenbart private domener
    emails = {e for e in emails if not any(e.endswith(x) for x in ("@gmail.com", "@outlook.com", "@hotmail.com", "@live.com"))}
    return sorted(emails)


def candidate_contact_paths() -> List[str]:
    return ["/", "/kontakt", "/om-oss", "/contact", "/about", "/kontakt-oss"]


def crawl_for_email(website: str) -> Optional[str]:
    base = normalize_url(website)
    if not base:
        return None
    parts = tldextract.extract(base)
    domain = ".".join(p for p in [parts.domain, parts.suffix] if p)

    for path in candidate_contact_paths()[:MAX_PAGES_PER_SITE]:
        html = fetch(base + path)
        if not html:
            continue
        emails = [e for e in extract_emails_from_html(html) if e.endswith("@" + domain) or e.split("@")[-1].endswith(domain)]
        if emails:
            # Prioriter generiske kontaktadresser
            preferred = sorted(emails, key=lambda e: (not any(k in e for k in ("kontakt@", "post@", "info@", "booking@", "bestilling@")), len(e)))
            return preferred[0]
        time.sleep(random.uniform(*CRAWL_DELAY))
    return None


# ---------------------------
# 3) Persistens og eksport
# ---------------------------

def upsert_companies(conn, companies: List[Company]):
    cur = conn.cursor()
    now = int(time.time())
    for c in companies:
        cur.execute(
            """
            INSERT INTO companies (orgnr, name, municipality, nace, website, email, source, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(orgnr) DO UPDATE SET
              name=excluded.name,
              municipality=excluded.municipality,
              nace=excluded.nace,
              website=COALESCE(excluded.website, companies.website),
              email=COALESCE(excluded.email, companies.email),
              source=excluded.source,
              last_seen=excluded.last_seen
            """,
            (c.orgnr, c.name, c.municipality, c.nace, c.website, c.email, c.source, now)
        )
    conn.commit()


def export_csv(conn, path=CSV_PATH, include_without_email: bool = False):
    cur = conn.cursor()
    if include_without_email:
        cur.execute("SELECT orgnr, name, municipality, nace, website, email FROM companies")
    else:
        cur.execute("SELECT orgnr, name, municipality, nace, website, email FROM companies WHERE email IS NOT NULL")
    rows = cur.fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["orgnr", "name", "municipality", "nace", "website", "email"])
        writer.writerows(rows)
    log(f"Exported {len(rows)} rows to {path}")


def export_names(conn, path="outreach_companies_names.csv"):
    cur = conn.cursor()
    cur.execute("SELECT orgnr, name, municipality, nace, website FROM companies ORDER BY name ASC")
    rows = cur.fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["orgnr", "name", "municipality", "nace", "website"])
        writer.writerows(rows)
    log(f"Exported {len(rows)} names to {path}")


# ---------------------------
# 4) Utsending via SendGrid API (anbefalt)
# ---------------------------

def render_template(template_str: str, context: Dict[str, str]) -> str:
    return Template(template_str).render(**context)


def send_via_sendgrid(to_email: str, subject: str, html_body: str, from_name: str, from_email: str, reply_to: Optional[str] = None):
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, From, To, ReplyTo, Content

    message = Mail(
        from_email=From(from_email, from_name),
        to_emails=[To(to_email)],
        subject=subject,
        html_content=html_body,
    )
    if reply_to:
        message.reply_to = ReplyTo(reply_to)

    sg = SendGridAPIClient(api_key=os.environ.get("SENDGRID_API_KEY"))
    resp = sg.send(message)
    if resp.status_code >= 300:
        raise RuntimeError(f"SendGrid error: {resp.status_code} {resp.body}")


def already_unsubscribed(conn, email: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM unsubscribed WHERE email=?", (email.lower(),))
    return cur.fetchone() is not None


def mark_sent(conn, email: str, orgnr: str):
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO sent (email, company_orgnr, sent_at) VALUES (?, ?, ?)", (email.lower(), orgnr, int(time.time())))
    conn.commit()


def send_campaign(conn, subject: str, body_template: str, limit=BATCH_LIMIT):
    from_name = os.getenv("OUTREACH_FROM_NAME", "Din Bedrift Catering")
    from_email = os.getenv("OUTREACH_FROM_EMAIL", "post@dittdomene.no")
    reply_to = os.getenv("OUTREACH_REPLY_TO", from_email)

    cur = conn.cursor()
    # Velg kandidater med e‑post, som ikke er sendt og ikke er unsubscribed
    cur.execute(
        """
        SELECT c.orgnr, c.name, c.municipality, c.website, c.email
        FROM companies c
        LEFT JOIN sent s ON lower(c.email) = lower(s.email)
        WHERE c.email IS NOT NULL AND s.email IS NULL
        LIMIT ?
        """,
        (limit,)
    )
    rows = cur.fetchall()

    for orgnr, name, municipality, website, email in rows:
        if already_unsubscribed(conn, email):
            continue
        context = {
            "company_name": name,
            "municipality": municipality,
            "website": website or "",
            "email": email,
        }
        html_body = render_template(body_template, context)
        try:
            send_via_sendgrid(email, subject, html_body, from_name, from_email, reply_to)
            mark_sent(conn, email, orgnr)
            log(f"SENT to {email} ({name})")
        except Exception as e:
            log(f"ERROR sending to {email}: {e}")
        time.sleep(random.uniform(*SEND_RATE_SECONDS))


# ---------------------------
# CLI-lignende kjeder
# ---------------------------
DEFAULT_TEMPLATE = """
<p>Hei {{ company_name }},</p>
<p>Vi leverer fersk lunsj og møtemat til bedrifter i området – fleksibelt og rimelig. Alt lages lokalt samme dag.</p>
<ul>
  <li>Daglig lunsjlevering eller ad hoc til møter</li>
  <li>Vegetar/veganske alternativer og allergitilpasning</li>
  <li>Levering i {{ municipality }}</li>
</ul>
<p>Ønsker dere meny og priser?</p>
<p>Vennlig hilsen<br/>
Din Bedrift Catering</p>
<p style="font-size:12px;color:#666">Hvis du ikke ønsker flere e‑poster fra oss, klikk her: <a href="{{ getenv('OUTREACH_UNSUBSCRIBE_URL', '') | default('#', true) | replace('{{email}}', email) }}">stopp utsendelser</a></p>
"""


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Local B2B outreach pipeline (NO)")
    parser.add_argument("action", choices=["fetch", "enrich", "export", "export-names", "send", "quick"], help="Hva skal gjøres")
    parser.add_argument("--municipalities", nargs="*", default=DEFAULT_MUNICIPALITY_NUMBERS, help="Kommunenummer, f.eks. 0301 for Oslo")
    parser.add_argument("--nace", nargs="*", default=TARGET_NACE_PREFIXES, help="NACE-prefiks, f.eks. 56 for servering")
    parser.add_argument("--max-pages", type=int, default=None, help="Maks antall sider å hente per kommune (for rask test)")
    parser.add_argument("--quiet", action="store_true", help="Mindre logging")
    parser.add_argument("--subject", default="Lunsj og møtemat levert lokalt")
    parser.add_argument("--template_path", default=None, help="HTML-mal (Jinja2). Hvis ikke satt, brukes DEFAULT_TEMPLATE")
    args = parser.parse_args()

    conn = init_db()

    if args.action == "fetch":
        comps = fetch_from_brreg(args.municipalities, args.nace, max_pages=args.max_pages, verbose=not args.quiet)
        upsert_companies(conn, comps)
        log(f"Fetched {len(comps)} companies from BRREG")

    elif args.action == "quick":
        # Quick test: fetch a single page and immediately export CSV
        comps = fetch_from_brreg(args.municipalities, args.nace, max_pages=1, verbose=not args.quiet)
        upsert_companies(conn, comps)
        export_csv(conn)
        log("Quick run complete (1 page per kommune).")

    elif args.action == "enrich":

        cur = conn.cursor()
        cur.execute("SELECT orgnr, website FROM companies WHERE email IS NULL")
        todo = cur.fetchall()
        count = 0
        for orgnr, website in todo:
            if not website:
                continue
            email = crawl_for_email(website)
            if email:
                cur.execute("UPDATE companies SET email=? WHERE orgnr=?", (email, orgnr))
                conn.commit()
                count += 1
                log(f"Found email {email} for orgnr {orgnr}")
                time.sleep(random.uniform(*CRAWL_DELAY))
        log(f"Enriched {count} companies with email")

    elif args.action == "export":
        # Default behavior kept: only rows with email
        export_csv(conn, include_without_email=False)

    elif args.action == "export-names":
        # New: export all companies regardless of email, just names + meta
        export_names(conn)

    elif args.action == "send":

        template_str = DEFAULT_TEMPLATE
        if args.template_path and os.path.exists(args.template_path):
            with open(args.template_path, "r", encoding="utf-8") as f:
                template_str = f.read()
        send_campaign(conn, args.subject, template_str)


if __name__ == "__main__":
    main()
