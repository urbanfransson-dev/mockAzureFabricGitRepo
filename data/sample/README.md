# Sample Data

This directory contains sample/seed data files used for:
- Unit and integration testing
- Local development against realistic data shapes
- Schema validation
- Pipeline development without connecting to live systems

## Files

| File | Description | Rows | Format |
|------|-------------|------|--------|
| `sales_orders.csv` | Sample sales order transactions | 15 | CSV |
| `customers.csv` | Customer master data | 10 | CSV |
| `products.json` | Product catalog from ERP | 6 | JSON |
| `crm_accounts_api_response.json` | Mocked Salesforce API response | 4 | JSON |

## Usage

These files are loaded automatically by the test suite via `conftest.py`.

To load manually in a notebook:

```python
import pandas as pd

orders = pd.read_csv("data/sample/sales_orders.csv", parse_dates=["order_date"])
customers = pd.read_csv("data/sample/customers.csv")
```

## Notes

- All customer names, order IDs, and financial figures are **fictional**.
- Phone numbers, emails, and addresses use `.example` TLD or placeholder formats.
- Do **not** add real customer data or PII to this directory.
