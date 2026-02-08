from pathlib import Path
import requests
import pandas as pd
import xml.etree.ElementTree as ET

DATA_DIR = Path("_data")
DATA_DIR.mkdir(exist_ok=True)

SOAP_URL = "https://cdr.ffiec.gov/public/pws/webservices/retrievalservice.asmx"
SOAP_ACTION = "http://cdr.ffiec.gov/public/services/RetrieveReportingPeriods"

# IMPORTANT: dataSeries is case-sensitive in practice; use "CALL" (FFIEC examples often use uppercase)
ENVELOPE = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap:Body>
    <RetrieveReportingPeriods xmlns="http://cdr.ffiec.gov/public/services">
      <dataSeries>Call</dataSeries>
    </RetrieveReportingPeriods>
  </soap:Body>
</soap:Envelope>
"""

def pull_reporting_periods() -> pd.DataFrame:
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": SOAP_ACTION,
        "User-Agent": "Mozilla/5.0",
    }
    r = requests.post(SOAP_URL, data=ENVELOPE.encode("utf-8"), headers=headers, timeout=60)
    print("STATUS:", r.status_code)
    print("HEADERS:", r.headers.get("Content-Type"))
    (Path("_data") / "ffiec_pws_debug.xml").write_text(r.text, encoding="utf-8", errors="ignore")
    r.raise_for_status()	
    # If FFIEC throws 500, dump response body to help diagnose
    if r.status_code >= 400:
        debug = DATA_DIR / "ffiec_pws_debug.xml"
        debug.write_text(r.text, encoding="utf-8", errors="ignore")
        r.raise_for_status()

    root = ET.fromstring(r.text)

    ns = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "p": "http://cdr.ffiec.gov/public/services",
    }
    strings = root.findall(".//p:RetrieveReportingPeriodsResult/p:string", ns)
    periods = [s.text for s in strings if s.text]

    df = pd.DataFrame({"reporting_period_end_date": periods})
    df["reporting_period_end_date"] = pd.to_datetime(df["reporting_period_end_date"], errors="coerce")
    df = df.dropna().sort_values("reporting_period_end_date")
    return df

if __name__ == "__main__":
    df = pull_reporting_periods()
    out = DATA_DIR / "ffiec_reporting_periods.parquet"
    df.to_parquet(out, index=False)
    print(f"Saved {len(df):,} rows -> {out}")

