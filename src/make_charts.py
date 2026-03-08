import matplotlib.pyplot as plt
from pathlib import Path

OUTPUT_DIR = Path("../_output")
OUTPUT_DIR.mkdir(exist_ok=True)

def bank_asset_distribution(df):

    plt.figure(figsize=(8,5))

    df["assets"].hist(bins=50)

    plt.title("Distribution of Bank Assets")
    plt.xlabel("Total Assets")
    plt.ylabel("Number of Banks")

    plt.tight_layout()

    path = OUTPUT_DIR / "bank_asset_distribution.png"
    plt.savefig(path)

    print(f"Chart saved to {path}")
