# Payment Failure Analysis & Revenue Recovery

## Overview
End-to-end data analytics project analyzing 99,441 real Brazilian e-commerce orders to identify payment failure patterns, quantify revenue leakage, and segment customers by payment risk.

## Business Problem
- Which payment methods fail the most?
- How much monthly revenue is at risk from failed payments?
- Which customers are repeatedly failing and should be flagged at checkout?
- How much failed revenue can be recovered through retry logic?

## Key Findings
- Average monthly revenue at risk: $2,997
- Voucher payments fail at 167% higher rates than credit card transactions (12% vs 4.5%)
- 1,234 high risk customers identified out of 99,441 total
- $50,059 in revenue recovered through retry logic simulation

## Tools Used
| Tool | Purpose |
|---|---|
| Python | Data loading and enrichment |
| PostgreSQL | Database |
| dbt | Data transformation |
| SQL | Analysis and querying |
| Tableau | Dashboard and visualization |

## Dataset
Brazilian E-Commerce Public Dataset by Olist
99,441 real orders 

https://public.tableau.com/app/profile/martha.grace.kommuguri2215/viz/Book1_17720288635240/Dashboard1?publish=yes
