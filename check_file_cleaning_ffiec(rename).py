import zipfile
import io
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats.mstats import winsorize
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "_data"
DATA_DIR.mkdir(exist_ok=True)

ZIP_PATH = DATA_DIR / "FFIEC CDR Call Bulk All Schedules 03312022.zip"

REPORT_DATE = "03312022"  # used to label outputs

def read_ffiec(zf, filename):
    with zf.open(filename) as f:
        df = pd.read_csv(f, sep='\t', header=0, skiprows=[1],
                         low_memory=False, dtype=str)
    df.columns = df.columns.str.strip().str.replace('"', '').str.lower()
    df = df.rename(columns={'idrssd': 'rssd9001'})
    df['rssd9001'] = pd.to_numeric(df['rssd9001'], errors='coerce')
    df = df.dropna(subset=['rssd9001'])
    df['rssd9001'] = df['rssd9001'].astype(int)
    df = df.set_index('rssd9001')
    df = df.apply(pd.to_numeric, errors='coerce')
    return df


def read_multipart(zf, base_name, n_parts):
    """Concatenate multi-part FFIEC files column-wise (same banks, split columns)."""
    parts = []
    for i in range(1, n_parts + 1):
        fname = f"{base_name}({i} of {n_parts}).txt"
        parts.append(read_ffiec(zf, fname))
    return pd.concat(parts, axis=1)

def fmt_dollar(num_thousands):
    """Format a number in thousands to B/T string."""
    num = num_thousands * 1000
    if num >= 1e12:
        return f"{num / 1e12:.1f}T"
    elif num >= 1e9:
        return f"{num / 1e9:.1f}B"
    else:
        return f"{num / 1e6:.1f}M"


with zipfile.ZipFile(ZIP_PATH) as zf:
    rc   = read_ffiec(zf, f"FFIEC CDR Call Schedule RC {REPORT_DATE}.txt")
    rca  = read_ffiec(zf, f"FFIEC CDR Call Schedule RCA {REPORT_DATE}.txt")
    rcb  = read_multipart(zf, f"FFIEC CDR Call Schedule RCB {REPORT_DATE}", 2)
    rcci = read_ffiec(zf, f"FFIEC CDR Call Schedule RCCI {REPORT_DATE}.txt")
    rce  = read_ffiec(zf, f"FFIEC CDR Call Schedule RCE {REPORT_DATE}.txt")

print(f"  RC:   {rc.shape[0]:,} banks, {rc.shape[1]} cols")
print(f"  RCB:  {rcb.shape[0]:,} banks, {rcb.shape[1]} cols")
print(f"  RCCI: {rcci.shape[0]:,} banks, {rcci.shape[1]} cols")
print(f"  RCE:  {rce.shape[0]:,} banks, {rce.shape[1]} cols")

rcfd_df = pd.concat([
    rc[[c for c in rc.columns if c.startswith('rcfd')]],
    rca[[c for c in rca.columns if c.startswith('rcfd')]],
    rcb[[c for c in rcb.columns if c.startswith('rcfd')]],
    rcci[[c for c in rcci.columns if c.startswith('rcfd')]],
], axis=1)

rcfd_df = rcfd_df.loc[:, ~rcfd_df.columns.duplicated()]

rcon_df = pd.concat([
    rc[[c for c in rc.columns if c.startswith('rcon')]],
    rcb[[c for c in rcb.columns if c.startswith('rcon')]],
    rcci[[c for c in rcci.columns if c.startswith('rcon')]],
    rce[[c for c in rce.columns if c.startswith('rcon')]],
], axis=1)

rcon_df = rcon_df.loc[:, ~rcon_df.columns.duplicated()]

rcfn_df = rc[[c for c in rc.columns if c.startswith('rcfn')]]

print(f"\nIntermediate tables:")
print(f"  rcfd_df: {rcfd_df.shape}")
print(f"  rcon_df: {rcon_df.shape}")
print(f"  rcfn_df: {rcfn_df.shape}")

print('rcfd2170' in rcfd_df.columns)   # total assets
print('rcfd2122' in rcfd_df.columns)   # total loans  
print('rcfdg301' in rcfd_df.columns)   # RMBS
print('rcon2200' in rcon_df.columns)   # domestic deposits
print('rconmt91' in rcon_df.columns)   # insured deposits
print('rcfn2200' in rcfn_df.columns)   # foreign deposits





# ─────────────────────────────────────────────
# SANITY CHECK: verify all columns exist before building tables
# ─────────────────────────────────────────────

# Find which schedule contains rcfd0010

def check_cols(df, cols, df_name):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"  ❌ {df_name} missing: {missing}")
    else:
        print(f"  ✅ {df_name} — all present")

print("Checking rcfd_df...")
check_cols(rcfd_df, ['rcfd2170','rcfd0010','rcfd1771','rcfd1773','rcfd0213','rcfd1287'], 'basic cols')
check_cols(rcfd_df, global_rmbs,   'global_rmbs')
check_cols(rcfd_df, global_cmbs,   'global_cmbs')
check_cols(rcfd_df, global_abs,    'global_abs')
check_cols(rcfd_df, global_other,  'global_other')
check_cols(rcfd_df, global_rs_loan, 'global_rs_loan')
check_cols(rcfd_df, global_rs_residential_loan, 'global_rs_residential_loan')
check_cols(rcfd_df, global_rs_commerical_loan,  'global_rs_commerical_loan')
check_cols(rcfd_df, global_rs_other_loan,       'global_rs_other_loan')
check_cols(rcfd_df, global_ci_loan,      'global_ci_loan')
check_cols(rcfd_df, global_consumer_loan,'global_consumer_loan')
check_cols(rcfd_df, ['rcfd1590','rcfd2122','rcfdb989'], 'other rcfd')

print("\nChecking rcon_df...")
check_cols(rcon_df, ['rcon2170','rconb987','rconb989','rcon2122','rcon1590'], 'basic cols')
check_cols(rcon_df, domestic_cash,    'domestic_cash')
check_cols(rcon_df, domestic_total,   'domestic_total')
check_cols(rcon_df, domestic_treasury,'domestic_treasury')
check_cols(rcon_df, domestic_rmbs,    'domestic_rmbs')
check_cols(rcon_df, domestic_cmbs,    'domestic_cmbs')
check_cols(rcon_df, domestic_abs,     'domestic_abs')
check_cols(rcon_df, domestic_other,   'domestic_other')
check_cols(rcon_df, domestic_rs_loan, 'domestic_rs_loan')
check_cols(rcon_df, domestic_rs_residential_loan, 'domestic_rs_residential_loan')
check_cols(rcon_df, domestic_rs_commerical_loan,  'domestic_rs_commerical_loan')
check_cols(rcon_df, domestic_rs_other_loan,       'domestic_rs_other_loan')
check_cols(rcon_df, domestic_ci_loan,       'domestic_ci_loan')
check_cols(rcon_df, domestic_consumer_loan, 'domestic_consumer_loan')
check_cols(rcon_df, domestic_non_rep_loan,  'domestic_non_rep_loan')

print("\nChecking rcfn_df...")
check_cols(rcfn_df, ['rcfn2200'], 'rcfn2200')








# ─────────────────────────────────────────────
# STEP 3: DEFINE COLUMN GROUPS
# Copied directly from replicators
# ─────────────────────────────────────────────

global_rmbs = ['rcfdg301','rcfdg303','rcfdg305','rcfdg307','rcfdg309','rcfdg311',
               'rcfdg313','rcfdg315','rcfdg317','rcfdg319','rcfdg321','rcfdg323']
global_cmbs = ['rcfdk143','rcfdk145','rcfdk147','rcfdk149','rcfdk151','rcfdk153',
               'rcfdk157']
global_abs   = ['rcfdc988','rcfdc027']
global_other = ['rcfd1738','rcfd1741','rcfd1743','rcfd1746']
global_rs_loan = ['rcfdf158','rcfdf159','rcfd1420','rcfd1797','rcfd5367','rcfd5368','rcfd1460','rcfdf160','rcfdf161']
global_rs_residential_loan = ['rcfd1420','rcfd1797','rcfd5367','rcfd5368','rcfd1460']
global_rs_commerical_loan  = ['rcfdf160','rcfdf161']
global_rs_other_loan       = ['rcfdf158','rcfdf159']
global_ci_loan      = ['rcfd1763','rcfd1764']
global_consumer_loan = ['rcfdb538','rcfdb539','rcfdk137','rcfdk207']

domestic_cash    = ['rcon0081','rcon0071']
domestic_total   = ['rcon1771','rcon1773']
domestic_treasury = ['rcon0213','rcon1287']
domestic_rmbs = ['rconht55','rconht57','rcong309','rcong311',
               'rcong313','rcong315','rcong317','rcong319','rcong321','rcong323']
domestic_cmbs = ['rconk143','rconk145','rconk147','rconk149','rconk151','rconk153',
               'rconk157']
domestic_abs   = ['rconc988','rconc027','rconht59','rconht61']
domestic_other = ['rcon1738','rcon1741','rcon1743','rcon1746']
domestic_rs_loan = ['rconf158','rconf159','rcon1420','rcon1797','rcon5367','rcon5368','rcon1460','rconf160','rconf161']
domestic_rs_residential_loan = ['rcon1420','rcon1797','rcon5367','rcon5368','rcon1460']
domestic_rs_commerical_loan  = ['rconf160','rconf161']
domestic_rs_other_loan       = ['rconf158','rconf159']
domestic_ci_loan      = ['rcon1766']
domestic_consumer_loan = ['rconb538','rconb539','rconk137','rconk207']
domestic_non_rep_loan  = ['rconj454','rconj464','rconj451']

insured_deposit  = ['rconhk05','rconmt91','rconmt87']
uninsured_long   = ['rconhk14','rconhk15']

# ─────────────────────────────────────────────
# STEP 4: BUILD ASSET TABLES
# Copied directly from replicators
# ─────────────────────────────────────────────

rcfd_data = pd.DataFrame(index=rcfd_df.index)
rcfd_data['Total Asset']          = rcfd_df['rcfd2170']
rcfd_data['cash']                 = rcfd_df['rcfd0010']
rcfd_data['security_total']       = rcfd_df['rcfd1771'] + rcfd_df['rcfd1773']
rcfd_data['security_treasury']    = rcfd_df['rcfd0213'] + rcfd_df['rcfd1287']
rcfd_data['security_rmbs']        = rcfd_df[global_rmbs].sum(axis=1)
rcfd_data['security_cmbs']        = rcfd_df[global_cmbs].sum(axis=1)
rcfd_data['security_abs']         = rcfd_df[global_abs].sum(axis=1)
rcfd_data['security_other']       = rcfd_df[global_other].sum(axis=1)
rcfd_data['Total_Loan']           = rcfd_df['rcfd2122']
rcfd_data['Real_Estate_Loan']     = rcfd_df[global_rs_loan].sum(axis=1)
rcfd_data['Residential_Mortgage'] = rcfd_df[global_rs_residential_loan].sum(axis=1)
rcfd_data['Commerical_Mortgage']  = rcfd_df[global_rs_commerical_loan].sum(axis=1)
rcfd_data['Other_Real_Estate_Mortgage'] = rcfd_df[global_rs_other_loan].sum(axis=1)
rcfd_data['Agri_Loan']            = rcfd_df['rcfd1590']
rcfd_data['Comm_Indu_Loan']       = rcfd_df[global_ci_loan].sum(axis=1)
rcfd_data['Consumer_Loan']        = rcfd_df[global_consumer_loan].sum(axis=1)
rcfd_data['Non_Rep_Loan']         = np.nan
rcfd_data['Fed_Fund_Sold']        = rcon_df['rconb987']
rcfd_data['Reverse_Repo']         = rcfd_df['rcfdb989']

rcon_data = pd.DataFrame(index=rcon_df.index)
rcon_data['Total Asset']          = rcon_df['rcon2170']
rcon_data['cash']                 = rcon_df[domestic_cash].sum(axis=1)
rcon_data['security_total']       = rcon_df[domestic_total].sum(axis=1)
rcon_data['security_treasury']    = rcon_df[domestic_treasury].sum(axis=1)
rcon_data['security_rmbs']        = rcon_df[domestic_rmbs].sum(axis=1)
rcon_data['security_cmbs']        = rcon_df[domestic_cmbs].sum(axis=1)
rcon_data['security_abs']         = rcon_df[domestic_abs].sum(axis=1)
rcon_data['security_other']       = rcon_df[domestic_other].sum(axis=1)
rcon_data['Total_Loan']           = rcon_df['rcon2122']
rcon_data['Real_Estate_Loan']     = rcon_df[domestic_rs_loan].sum(axis=1)
rcon_data['Residential_Mortgage'] = rcon_df[domestic_rs_residential_loan].sum(axis=1)
rcon_data['Commerical_Mortgage']  = rcon_df[domestic_rs_commerical_loan].sum(axis=1)
rcon_data['Other_Real_Estate_Mortgage'] = rcon_df[domestic_rs_other_loan].sum(axis=1)
rcon_data['Agri_Loan']            = rcon_df['rcon1590']
rcon_data['Comm_Indu_Loan']       = rcon_df[domestic_ci_loan].sum(axis=1)
rcon_data['Consumer_Loan']        = rcon_df[domestic_consumer_loan].sum(axis=1)
rcon_data['Non_Rep_Loan']         = rcon_df[domestic_non_rep_loan].sum(axis=1)
rcon_data['Fed_Fund_Sold']        = rcon_df['rconb987']
rcon_data['Reverse_Repo']         = rcon_df['rconb989']

# ─────────────────────────────────────────────
# STEP 5: MERGE INTO bank_asset
# Copied directly from replicators
# ─────────────────────────────────────────────

bank_asset = pd.merge(rcfd_data, rcon_data, left_index=True, right_index=True,
                      how='outer', suffixes=('', '_df2'))
replace_index = bank_asset[bank_asset['cash'].isna()].index
bank_asset.loc[replace_index, bank_asset.columns[:19]] = bank_asset.loc[replace_index, bank_asset.columns[19:]].values
bank_asset.drop(columns='Non_Rep_Loan', inplace=True)
bank_asset.rename(columns={'Non_Rep_Loan_df2': 'Non_Rep_Loan'}, inplace=True)
columns_to_drop = [col for col in bank_asset.columns if '_df2' in col]
bank_asset.drop(columns=columns_to_drop, inplace=True)

print(f"bank_asset: {bank_asset.shape}")
print(bank_asset.head())


#sanity checks
print(f"Rows:    {bank_asset.shape[0]:,}")   # should be 4,844
print(f"Cols:    {bank_asset.shape[1]}")      # should be 19
print(f"Total assets ($T): {bank_asset['Total Asset'].sum() / 1e6:.1f}")  # should be ~24T
print(f"Any duplicate index: {bank_asset.index.duplicated().sum()}")      # should be 0
print(f"Banks with no cash data: {bank_asset['cash'].isna().sum()}")      # should be 0




#liability
# ─────────────────────────────────────────────
# STEP 6: BUILD LIABILITY TABLES
# Copied directly from replicators
# ─────────────────────────────────────────────

global_liability = pd.DataFrame(index=rcon_df.index)
global_liability['Total Liability']                    = rcfd_df['rcfd2948']
global_liability['Domestic Deposit']                   = rcon_df['rcon2200']
global_liability['Insured Deposit']                    = rcon_df[insured_deposit].sum(axis=1)
global_liability['Uninsured Deposit']                  = global_liability['Domestic Deposit'] - global_liability['Insured Deposit']
global_liability['Uninsured Time Deposits']            = rcon_df['rconj474']
global_liability['Uninsured Long-Term Time Deposits']  = rcon_df[uninsured_long].sum(axis=1)
global_liability['Uninsured Short-Term Time Deposits'] = rcon_df['rconk222']
global_liability['Foreign Deposit']                    = rcfn_df['rcfn2200']
global_liability['Fed Fund Purchase']                  = rcon_df['rconb993']
global_liability['Repo']                               = rcon_df['rconb995']
global_liability['Other Liability']                    = rcfd_df['rcfd2930']
global_liability['Total Equity']                       = rcfd_df['rcfdg105']
global_liability['Common Stock']                       = rcfd_df['rcfd3230']
global_liability['Preferred Stock']                    = rcfd_df['rcfd3838']
global_liability['Retained Earning']                   = rcfd_df['rcfd3632']

domestic_liability = pd.DataFrame(index=rcon_df.index)
domestic_liability['Total Liability']                    = rcon_df['rcon2948']
domestic_liability['Domestic Deposit']                   = rcon_df['rcon2200']
domestic_liability['Insured Deposit']                    = rcon_df[insured_deposit].sum(axis=1)
domestic_liability['Uninsured Deposit']                  = domestic_liability['Domestic Deposit'] - domestic_liability['Insured Deposit']
domestic_liability['Uninsured Time Deposits']            = rcon_df['rconj474']
domestic_liability['Uninsured Long-Term Time Deposits']  = rcon_df[uninsured_long].sum(axis=1)
domestic_liability['Uninsured Short-Term Time Deposits'] = rcon_df['rconk222']
domestic_liability['Foreign Deposit']                    = rcfn_df['rcfn2200']
domestic_liability['Fed Fund Purchase']                  = rcon_df['rconb993']
domestic_liability['Repo']                               = rcon_df['rconb995']
domestic_liability['Other Liability']                    = rcon_df['rcon2930']
domestic_liability['Total Equity']                       = rcon_df['rcong105']
domestic_liability['Common Stock']                       = rcon_df['rcon3230']
domestic_liability['Preferred Stock']                    = rcon_df['rcon3838']
domestic_liability['Retained Earning']                   = rcon_df['rcon3632']

# ─────────────────────────────────────────────
# STEP 7: MERGE INTO bank_liability
# Copied directly from replicators
# ─────────────────────────────────────────────

bank_liability = pd.merge(global_liability, domestic_liability, left_index=True, right_index=True,
                          how='outer', suffixes=('', '_df2'))
replace_index = bank_liability[bank_liability['Total Liability'].isna()].index
bank_liability.loc[replace_index, bank_liability.columns[:15]] = bank_liability.loc[replace_index, bank_liability.columns[15:]].values
columns_to_drop = [col for col in bank_liability.columns if '_df2' in col]
bank_liability.drop(columns=columns_to_drop, inplace=True)

print(f"bank_liability: {bank_liability.shape}")
print(bank_liability.head())

#sanity checks
print(f"Rows: {bank_liability.shape[0]:,}")          # should be 4,844
print(f"Cols: {bank_liability.shape[1]}")             # should be 15
print(f"Duplicate index: {bank_liability.index.duplicated().sum()}")  # should be 0
print(f"Total liabilities ($T): {bank_liability['Total Liability'].sum() / 1e6:.1f}")  # should be ~22T
print(f"Total equity ($T):      {bank_liability['Total Equity'].sum() / 1e6:.1f}")     # should be ~2T


# ─────────────────────────────────────────────
# STEP 8: BANK CLASSIFICATION
# Copied directly from replicators
# ─────────────────────────────────────────────

threshold = 1.384e6  # $1.384 billion in thousands

bank_asset['Bank Category'] = 0
bank_asset.loc[bank_asset['Total Asset'] >= threshold, 'Bank Category'] = 1

GSIB = [934329,488318,212465,449038,476810,3382547,852218,651448,480228,1443266,
        413208,3357620,1015560,2980209,214807,304913,670560,2325882,2182786,3066025,
        398668,541101,229913,1456501,2489805,722777,35301,93619,352745,812164,925411,
        3212149,451965,688079,1225761,2362458,2531991]

bank_asset.loc[bank_asset.index.isin(GSIB), 'Bank Category'] = 2

print(f"Small banks: {(bank_asset['Bank Category']==0).sum():,}")
print(f"Large banks: {(bank_asset['Bank Category']==1).sum():,}")
print(f"GSIBs:       {(bank_asset['Bank Category']==2).sum():,}")



# ─────────────────────────────────────────────
# STEP 9: SUMMARY STATS (Assets)
# Copied directly from replicators
# ─────────────────────────────────────────────

test_df = pd.DataFrame()
test_df['Aggregate'] = (bank_asset.sum() / bank_asset['Total Asset'].sum()) * 100
test_df['Full sample(mean)'] = (bank_asset.iloc[:, :-1].div(bank_asset['Total Asset'], axis=0) * 100).apply(lambda x: winsorize(x, limits=[0.05, 0.05])).mean()
test_df['Full sample(sd)']   = (bank_asset.iloc[:, :-1].div(bank_asset['Total Asset'], axis=0) * 100).apply(lambda x: winsorize(x, limits=[0.05, 0.05])).std()

bank_asset_small = bank_asset[bank_asset['Bank Category'] == 0]
test_df['small(mean)'] = (bank_asset_small.iloc[:, :-1].div(bank_asset_small['Total Asset'], axis=0) * 100).apply(lambda x: winsorize(x, limits=[0.05, 0.05])).mean()
test_df['small(sd)']   = (bank_asset_small.iloc[:, :-1].div(bank_asset_small['Total Asset'], axis=0) * 100).apply(lambda x: winsorize(x, limits=[0.05, 0.05])).std()

bank_asset_large = bank_asset[bank_asset['Bank Category'] == 1]
test_df['large(mean)'] = (bank_asset_large.iloc[:, :-1].div(bank_asset_large['Total Asset'], axis=0) * 100).apply(lambda x: winsorize(x, limits=[0.05, 0.05])).mean()
test_df['large(sd)']   = (bank_asset_large.iloc[:, :-1].div(bank_asset_large['Total Asset'], axis=0) * 100).apply(lambda x: winsorize(x, limits=[0.05, 0.05])).std()

bank_asset_GSIB = bank_asset[bank_asset['Bank Category'] == 2]
test_df['GSIB(mean)'] = (bank_asset_GSIB.iloc[:, :-1].div(bank_asset_GSIB['Total Asset'], axis=0) * 100).apply(lambda x: winsorize(x, limits=[0.05, 0.05])).mean()
test_df['GSIB(sd)']   = (bank_asset_GSIB.iloc[:, :-1].div(bank_asset_GSIB['Total Asset'], axis=0) * 100).apply(lambda x: winsorize(x, limits=[0.05, 0.05])).std()

test_df = test_df.round(1)
print(test_df)


# ─────────────────────────────────────────────
# STEP 10: CLEAN UP TABLE
# Copied directly from replicators
# ─────────────────────────────────────────────

def large_num(num):
    num = num * 1000
    if num < 1_000_000_000:
        return f"{num / 1_000_000_000:.1f}B" if num >= 100_000_000 else str(num)
    else:
        return f"{num / 1_000_000_000_000:.1f}T" if num >= 1_000_000_000_000 else f"{num / 1_000_000_000:.1f}B"

# Convert to object dtype so we can mix strings and numbers
test_df = test_df.astype(object)

test_df.loc['Total Asset', 'Aggregate']         = large_num(bank_asset['Total Asset'].sum())
test_df.loc['Total Asset', 'Full sample(mean)'] = large_num(bank_asset['Total Asset'].mean())
test_df.loc['Total Asset', 'Full sample(sd)']   = large_num(bank_asset['Total Asset'].std())
test_df.loc['Total Asset', 'small(mean)']       = large_num(bank_asset[bank_asset['Bank Category']==0]['Total Asset'].mean())
test_df.loc['Total Asset', 'small(sd)']         = large_num(bank_asset[bank_asset['Bank Category']==0]['Total Asset'].std())
test_df.loc['Total Asset', 'large(mean)']       = large_num(bank_asset[bank_asset['Bank Category']==1]['Total Asset'].mean())
test_df.loc['Total Asset', 'large(sd)']         = large_num(bank_asset[bank_asset['Bank Category']==1]['Total Asset'].std())
test_df.loc['Total Asset', 'GSIB(mean)']        = large_num(bank_asset[bank_asset['Bank Category']==2]['Total Asset'].mean())
test_df.loc['Total Asset', 'GSIB(sd)']          = large_num(bank_asset[bank_asset['Bank Category']==2]['Total Asset'].std())

# Add number of banks row
test_df.loc['N Banks', 'Aggregate']         = len(bank_asset)
test_df.loc['N Banks', 'Full sample(mean)'] = len(bank_asset)
test_df.loc['N Banks', 'small(mean)']       = (bank_asset['Bank Category']==0).sum()
test_df.loc['N Banks', 'large(mean)']       = (bank_asset['Bank Category']==1).sum()
test_df.loc['N Banks', 'GSIB(mean)']        = (bank_asset['Bank Category']==2).sum()

# Drop Bank Category row
test_df = test_df.drop(index='Bank Category')
test_df = test_df.fillna('')

print(test_df)


# ─────────────────────────────────────────────
# STEP 11: SUMMARY STATS (Liabilities)
# Copied directly from replicators
# ─────────────────────────────────────────────

bank_liability = bank_liability.join(bank_asset[['Bank Category']], how='left')
bank_liability = bank_liability.join(bank_asset[['Total Asset']], how='left')

df2 = pd.DataFrame()
df2['Aggregate']         = (bank_liability.sum() / bank_liability['Total Asset'].sum()) * 100
df2['Full sample(mean)'] = (bank_liability.iloc[:, :-1].div(bank_liability['Total Asset'], axis=0) * 100).apply(lambda x: pd.Series(np.array(winsorize(x.dropna(), limits=[0.05,0.05])), dtype=float).mean())
df2['Full sample(sd)']   = (bank_liability.iloc[:, :-1].div(bank_liability['Total Asset'], axis=0) * 100).apply(lambda x: pd.Series(np.array(winsorize(x.dropna(), limits=[0.05,0.05])), dtype=float).std())

bank_liability_small = bank_liability[bank_liability['Bank Category'] == 0]
df2['small(mean)'] = (bank_liability_small.iloc[:, :-1].div(bank_liability_small['Total Asset'], axis=0) * 100).apply(lambda x: pd.Series(np.array(winsorize(x.dropna(), limits=[0.05,0.05])), dtype=float).mean())
df2['small(sd)']   = (bank_liability_small.iloc[:, :-1].div(bank_liability_small['Total Asset'], axis=0) * 100).apply(lambda x: pd.Series(np.array(winsorize(x.dropna(), limits=[0.05,0.05])), dtype=float).std())

bank_liability_large = bank_liability[bank_liability['Bank Category'] == 1]
df2['large(mean)'] = (bank_liability_large.iloc[:, :-1].div(bank_liability_large['Total Asset'], axis=0) * 100).apply(lambda x: pd.Series(np.array(winsorize(x.dropna(), limits=[0.05,0.05])), dtype=float).mean())
df2['large(sd)']   = (bank_liability_large.iloc[:, :-1].div(bank_liability_large['Total Asset'], axis=0) * 100).apply(lambda x: pd.Series(np.array(winsorize(x.dropna(), limits=[0.05,0.05])), dtype=float).std())

bank_liability_GSIB = bank_liability[bank_liability['Bank Category'] == 2]
df2['GSIB(mean)'] = (bank_liability_GSIB.iloc[:, :-1].div(bank_liability_GSIB['Total Asset'], axis=0) * 100).apply(lambda x: pd.Series(np.array(winsorize(x.dropna(), limits=[0.05,0.05])), dtype=float).mean())
df2['GSIB(sd)']   = (bank_liability_GSIB.iloc[:, :-1].div(bank_liability_GSIB['Total Asset'], axis=0) * 100).apply(lambda x: pd.Series(np.array(winsorize(x.dropna(), limits=[0.05,0.05])), dtype=float).std())

df2 = df2.drop(index=['Total Asset', 'Bank Category'], errors='ignore')
df2 = df2.fillna(0).round(1)

print(df2)
