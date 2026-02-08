# Dataframe: `P0:repo_public` - Public Repo Data


## Description

This dataframe contains, among other things,repo rates and the Federal Funds target range. The shaded area shows the Federal Funds target range. During 2019, the Secured Overnight Financing Rate (SOFR), a broad measure of the cost of borrowing cash overnight collateralized by Treasury securities via repurchase agreements, often exceeded the upper limit of the Federal Funds target range. Since SOFR does not extend back further than 2017, we use the average repo rate in the triparty repo market whenever SOFR is unavailable.


## Data Dictionary

- **DATE**: `datetime64[ns]`
- **GDP**: `float64` US Nominal GDP
- **CPIAUCNS**: `float64` Consumer Price Index for All Urban Consumers: All Items in U.S. City Average https://fred.stlouisfed.org/series/CPIAUCNS
- **GDPC1**: `float64`  Real Gross Domestic Product 
- **DPCREDIT**: `float64`  Discount Window Primary Credit Rate
- **EFFR**: `float64` Effective Federal Funds Rate
- **OBFR**: `float64` Overnight Bank Funding Rate 
- **SOFR**: `float64` Secured Overnight Financing Rate
- **IORR**: `float64` Interest Rate on Required Reserves (IORR Rate) (DISCONTINUED)
- **IOER**: `float64` Interest Rate on Excess Reserves (IOER Rate) (DISCONTINUED)
- **IORB**: `float64` Interest Rate on Reserve Balances (IORB Rate)
- **Fed_Funds_Target_Upper**: `float64`
- **Fed_Funds_Target_Lower**: `float64`
- **WALCL**: `float64` Assets: Total Assets: Total Assets (Less Eliminations from Consolidation): Wednesday Level 
- **TOTRESNS**: `float64` Reserves of Depository Institutions: Total 
- **TREAST**: `float64` Assets: Securities Held Outright: U.S. Treasury Securities: All: Wednesday Level
- **CURRCIR**: `float64` Currency in Circulation
- **GFDEBTN**: `float64` Federal Debt: Total Public Debt
- **WTREGEN**: `float64` Liabilities and Capital: Liabilities: Deposits with F.R. Banks, Other Than Reserve Balances: U.S. Treasury, General Account: Week Average
- **ON_RRP_Facility_Rate**: `float64` Overnight Reverse Repo Facility Rate
- **RRPONTSYD**: `float64` Overnight Reverse Repurchase Agreements: Treasury Securities Sold by the Federal Reserve in the Temporary Open Market Operations
- **RPONTSYD**: `float64` Overnight Repurchase Agreements: Treasury Securities Purchased by the Federal Reserve in the Temporary Open Market Operations 
- **WSDONTL**: `float64` Memorandum Items: Securities Lent to Dealers: Overnight Facility: Wednesday Level
- **Interest_on_Reserves**: `float64` IORB rate, backfilled with IOER reserves when IORB is not available.
- **ONRRP_CTPY_LIMIT**: `float64` Overnight Reverse Repo Facility Counterparty Limits
- **ONRP_AGG_LIMIT**: `float64` Standing Repo Facility (SRF) aggregate limit, not updated since late 2023
- **Tri_Party_Overnight_Average_Rate**: `float64`
- **REPO_TRI_TV_OO_P**: `float64` Triparty overnight transaction volume
- **REPO_TRI_TV_TOT_P**: `float64` Triparty transaction volume on all repos
- **REPO_DVP_AR_OO_P**: `float64` DVP average rate on overnight repos
- **REPO_DVP_TV_OO_P**: `float64` DVP transaction volume on overnight repos
- **REPO_DVP_TV_TOT_P**: `float64` DVP transaction volume on all repos
- **REPO_DVP_OV_TOT_P**: `float64` DVP outstanding volume on all repos  
- **REPO_GCF_AR_OO_P**: `float64` GCF average rate on overnight repos
- **REPO_GCF_TV_OO_P**: `float64` GCF transaction volume on overnight repos
- **REPO_GCF_TV_TOT_P**: `float64` GCF transaction volume on all repos
- **FNYR_BGCR_A**: `float64` Federal Reserve Bank of New York Reference Rates. Broad General Collateral Rate
- **FNYR_TGCR_A**: `float64` Federal Reserve Bank of New York Reference Rates. Tri-Party General Collateral Rate
- **target_midpoint**: `float64` Fed Funds target midpoint
- **SOFR_less_IORB**: `float64` SOFR less interest on reserve balances (backfilled with IOER)
- **Fed_Balance_Sheet_over_GDP**: `float64` Size of Fed's balance sheet divided by nominal GDP
- **Tri_Party_less_Fed_ON_RRP_Rate**: `float64` Triparty average rate less the Fed's overnight reverse repurchase agreement facility awared rate
- **Tri_Party_Rate_Less_Fed_Funds_Upper_Limit**: `float64` Triparty average rate less the Fed Fund's target upper limit
- **Tri_Party_Rate_Less_Fed_Funds_Midpoint**: `float64` Triparty average rate less the Fed Fund's target range midpoint
- **net_fed_repo**: `float64` `RPONTSYD - RRPONTSYD`, Fed's total repos minus Fed's reverse repos
- **Total_Reserves_over_Currency**: `float64` Reserves of Depository Institutions: Total, divided by currency in circulation
- **Total_Reserves_over_GDP**: `float64` Reserves of Depository Institutions: Total, divided by nominal GDP
- **SOFR_extended_with_Triparty**: `float64` SOFR only goes back to around 2017. Before then, I use the average triparty repo rate to backfill.




## DataFrame Glimpse

```
Rows: 9694
Columns: 45
$ GDP                                                <f64> None
$ CPIAUCNS                                           <f64> None
$ GDPC1                                              <f64> None
$ DPCREDIT                                           <f64> 3.75
$ EFFR                                               <f64> None
$ OBFR                                               <f64> 3.63
$ SOFR                                               <f64> None
$ Fed_Funds_Target_Upper                             <f64> 3.75
$ Fed_Funds_Target_Lower                             <f64> 3.5
$ WALCL                                              <f64> 6605.909
$ TOTRESNS                                           <f64> 2941.7
$ TREAST                                             <f64> 4281.018
$ CURRCIR                                            <f64> 2415.653
$ GFDEBTN                                            <f64> None
$ WTREGEN                                            <f64> 908773.0
$ ON_RRP_Facility_Rate                               <f64> 3.5
$ RRPONTSYD                                          <f64> None
$ RPONTSYD                                           <f64> None
$ WSDONTL                                            <f64> 39.029
$ Interest_on_Reserves                               <f64> 3.65
$ ONRRP_CTPY_LIMIT                                   <f64> 160.0
$ ONRP_AGG_LIMIT                                     <f64> 500.0
$ Tri_Party_Overnight_Average_Rate                   <f64> None
$ REPO_TRI_TV_OO_P                                   <f64> None
$ REPO_TRI_TV_TOT_P                                  <f64> None
$ REPO_DVP_AR_OO_P                                   <f64> None
$ REPO_DVP_TV_OO_P                                   <f64> None
$ REPO_DVP_TV_TOT_P                                  <f64> None
$ REPO_DVP_OV_TOT_P                                  <f64> None
$ REPO_GCF_AR_OO_P                                   <f64> None
$ REPO_GCF_TV_OO_P                                   <f64> None
$ REPO_GCF_TV_TOT_P                                  <f64> None
$ FNYR_BGCR_A                                        <f64> None
$ FNYR_TGCR_A                                        <f64> None
$ target_midpoint                                    <f64> 3.625
$ SOFR_less_IORB                                     <f64> None
$ Fed_Balance_Sheet_over_GDP                         <f64> 0.21242212568662314
$ Tri_Party_less_Fed_ON_RRP_Rate                     <f64> None
$ Tri_Party_Rate_Less_Fed_Funds_Upper_Limit          <f64> None
$ Tri_Party_Rate_Less_Fed_Funds_Midpoint             <f64> None
$ net_fed_repo                                       <f64> None
$ Total_Reserves_over_Currency                       <f64> 1.2177659622470611
$ Total_Reserves_over_GDP                            <f64> None
$ SOFR_extended_with_Triparty                        <f64> None
$ date                                      <datetime[ns]> 2026-02-07 00:00:00


```

## Dataframe Manifest

| Dataframe Name                 | Public Repo Data                                                   |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [repo_public](../dataframes/P0/repo_public.md)                                       |
| Data Sources                   | FRED, Office of Financial Research                                        |
| Data Providers                 | FRED, Office of Financial Research                                      |
| Links to Providers             | https://fred.stlouisfed.org/, https://www.financialresearch.gov/short-term-funding-monitor/api/                             |
| Topic Tags                     | Short Term Funding, Repo                                          |
| Type of Data Access            | P,u,b,l,i,c                                  |
| How is data pulled?            | Web API via Python                                                    |
| Data available up to (min)     | 2025-07-01 00:00:00                                                             |
| Data available up to (max)     | 2026-02-07 00:00:00                                                             |
| Dataframe Path                 | /Users/AnoushkaGehani/Desktop/p08_jiang_et_al_2024/_data/repo_public.parquet                                                   |


**Linked Charts:**


- [P0:repo_rates](../../charts/P0.repo_rates.md)



## Pipeline Manifest

| Pipeline Name                   | Bank Fragility Monitor                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [P0](../index.md)              |
| Lead Pipeline Developer         | Anoushka Gehani and Hashir Bawany             |
| Contributors                    | Anoushka Gehani and Hashir Bawany           |
| Git Repo URL                    |                         |
| Pipeline Web Page               | <a href="file:///Users/AnoushkaGehani/Desktop/p08_jiang_et_al_2024/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-02-07 22:43:30           |
| OS Compatibility                |  |
| Linked Dataframes               |  [P0:repo_public](../dataframes/P0/repo_public.md)<br>  |


