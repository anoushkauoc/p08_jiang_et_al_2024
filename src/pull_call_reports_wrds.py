from pathlib import Path
import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
DATA_DIR = Path("_data")
DATA_DIR.mkdir(exist_ok=True)

def main():
    user = os.getenv("WRDS_USERNAME")
    if not user:
        raise ValueError("WRDS_USERNAME not set. Put it in .env or export it.")

    # TODO: replace this section with the professor-provided WRDS call report pull code.
    # For HW3 you can start by pulling a tiny table (even 100 rows) and saving parquet.
    #
    # Example pattern (pseudo-code):
    # import wrds
    # db = wrds.Connection(wrds_username=user)
    # df = db.raw_sql("select * from bank.call_reports_table limit 100")
    # df.to_parquet(DATA_DIR / "call_reports_sample.parquet", index=False)

    # Temporary dummy dataframe so you can finish chartbook wiring immediately:
    df = pd.DataFrame({"note": ["Replace with WRDS pull code"], "n": [1]})
    df.to_parquet(DATA_DIR / "call_reports_sample.parquet", index=False)
    print("Wrote _data/call_reports_sample.parquet (placeholder)")

if __name__ == "__main__":
    main()

