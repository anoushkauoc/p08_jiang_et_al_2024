from pathlib import Path
import pandas as pd
import plotly.express as px

df = pd.read_parquet("_data/call_reports_sample.parquet")

# If it's the placeholder, just plot n
if "n" in df.columns:
    fig = px.bar(df, x="note", y="n", title="Call Reports Sample (placeholder)")
else:
    # Otherwise: pick first numeric col and histogram it
    num = df.select_dtypes("number").columns
    col = num[0]
    fig = px.histogram(df, x=col, title=f"Call Reports Sample: Distribution of {col}")

Path("_output").mkdir(exist_ok=True)
fig.write_html("_output/call_reports_sample.html")
print("Wrote _output/call_reports_sample.html")

