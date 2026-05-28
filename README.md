# ADF UPS Finance & Billing — Sample Project

A complete Azure Data Factory project for **UPS Finance & Billing** domain.
Processes CSV source files through transformation and aggregation data flows,
and sinks results into output CSV files.

---

## Project Structure

```
adf-ups-finance/
├── linkedService/
│   ├── LS_AzureBlobStorage_Source.json      ← Source storage connection
│   └── LS_AzureBlobStorage_Sink.json        ← Output storage connection
├── dataset/
│   ├── DS_CSV_BillingTransactions_Source.json
│   ├── DS_CSV_CustomerAccounts_Source.json
│   ├── DS_CSV_InvoiceSummary_Source.json
│   ├── DS_CSV_BillingReport_Sink.json
│   └── DS_CSV_CustomerBillingSummary_Sink.json
├── dataflow/
│   ├── DF_BillingTransactions_Transform.json
│   └── DF_InvoiceSummary_Transform.json
├── pipeline/
│   ├── PL_UPS_Finance_Master.json           ← Start here (orchestrator)
│   ├── PL_UPS_BillingTransactions_ETL.json
│   └── PL_UPS_InvoiceSummary_ETL.json
└── trigger/
    └── TR_UPS_Finance_Daily_Schedule.json
```

---

## Data Flow Architecture

```
Source CSVs (Azure Blob - ups-finance-source)
        │
        ├── billing/raw/billing_transactions.csv  ──┐
        ├── customers/raw/customer_accounts.csv   ──┤──► DF_BillingTransactions_Transform
        │                                            │         │
        └── invoices/raw/invoice_summary.csv ───────┘    DS_CSV_BillingReport_Sink
                │                                              │
                └──► DF_InvoiceSummary_Transform          Output CSV
                              │
                         DS_CSV_CustomerBillingSummary_Sink
                              │
                         Output CSV
```

---

## Source CSV Schemas

### billing_transactions.csv
| Column | Type | Description |
|---|---|---|
| TransactionID | String | Unique transaction identifier |
| CustomerID | String | Customer reference key |
| ShipmentID | String | UPS shipment tracking ID |
| BillingDate | Date (yyyy-MM-dd) | Date of billing |
| ServiceType | String | e.g. GROUND, 2DAY_AIR, OVERNIGHT |
| WeightLbs | Decimal | Package weight in pounds |
| Zone | String | UPS delivery zone (1–8) |
| BaseCharge | Decimal | Base shipping charge |
| FuelSurcharge | Decimal | Fuel surcharge amount |
| ResidentialFee | Decimal | Residential delivery surcharge |
| DeclaredValue | Decimal | Declared package value |
| TotalCharge | Decimal | Total billed amount |
| Currency | String | e.g. USD |
| PaymentStatus | String | PAID, PENDING, OVERDUE |

### customer_accounts.csv
| Column | Type | Description |
|---|---|---|
| CustomerID | String | Unique customer identifier |
| AccountNumber | String | UPS account number |
| CompanyName | String | Business name |
| ContactName | String | Primary contact |
| Email | String | Billing email |
| Phone | String | Contact phone |
| BillingAddress | String | Street address |
| City | String | City |
| State | String | State/Province |
| ZipCode | String | ZIP/Postal code |
| Country | String | Country code (e.g. US) |
| AccountType | String | STANDARD, PREMIUM, ENTERPRISE |
| CreditLimit | Decimal | Account credit limit |
| AccountStatus | String | ACTIVE, SUSPENDED, CLOSED |

### invoice_summary.csv
| Column | Type | Description |
|---|---|---|
| InvoiceID | String | Unique invoice identifier |
| CustomerID | String | Customer reference key |
| InvoiceDate | Date (yyyy-MM-dd) | Invoice issue date |
| DueDate | Date (yyyy-MM-dd) | Payment due date |
| BillingPeriodStart | Date (yyyy-MM-dd) | Billing period start |
| BillingPeriodEnd | Date (yyyy-MM-dd) | Billing period end |
| TotalShipments | Integer | Shipment count in period |
| TotalWeight | Decimal | Total weight shipped |
| SubTotal | Decimal | Pre-tax subtotal |
| TaxAmount | Decimal | Tax applied |
| DiscountAmount | Decimal | Discounts applied |
| InvoiceTotal | Decimal | Final invoice amount |
| Currency | String | e.g. USD |
| InvoiceStatus | String | PAID, PENDING, OVERDUE |

---

## Import Steps (GitHub → Azure Data Factory)

### 1. Push to GitHub
```bash
git init
git add .
git commit -m "Initial UPS Finance ADF project"
git remote add origin https://github.com/<your-username>/adf-ups-finance.git
git push -u origin main
```

### 2. Connect ADF to GitHub
1. Open Azure Data Factory Studio
2. Go to **Manage → Git configuration**
3. Select **GitHub** as the repository type
4. Authorize and select your repository `adf-ups-finance`
5. Set **Collaboration branch** = `main`

### 3. Configure Linked Services (Manual Step)
> ⚠️ You must update connection strings manually before publishing.

In each linked service JSON, replace:
- `<your-storage-account-key>` with your actual Azure Storage account key
- Update `AccountName` to match your actual storage account name

### 4. Configure Integration Runtime (Manual Step)
> ⚠️ Integration Runtime is NOT included in these JSON files per your requirement.

After importing, go to **Manage → Integration Runtimes** and:
- Create an **Azure IR** (for cloud-to-cloud) — recommended
- Or create a **Self-hosted IR** (if source files are on-premises)
- Assign it to your linked services under each linked service's **Connect via integration runtime** setting.

### 5. Create Blob Storage Containers
In your Azure Storage account, create:
- Container: `ups-finance-source`
  - Folder: `billing/raw/`  → upload `billing_transactions.csv`
  - Folder: `customers/raw/` → upload `customer_accounts.csv`
  - Folder: `invoices/raw/`  → upload `invoice_summary.csv`
- Container: `ups-finance-output`
  - Folder: `billing/aggregated/` (auto-created on first run)
  - Folder: `customers/summary/`   (auto-created on first run)

### 6. Publish and Test
1. Click **Publish All** in ADF Studio
2. Go to **PL_UPS_Finance_Master** pipeline
3. Click **Debug** to do a test run
4. Monitor progress in **Monitor → Pipeline Runs**

---

## Transformations Performed

### DF_BillingTransactions_Transform
| Step | Operation |
|---|---|
| FilterValidTransactions | Drop rows with null TransactionID or zero TotalCharge |
| CastDataTypes | Parse date, weight, and monetary strings to typed values |
| DeriveCalculatedColumns | Compute TotalSurcharges, NetBillingAmount, BillingMonth, BillingYear |
| JoinCustomerData | Left join with customer_accounts on CustomerID |
| AggregateByCustomerAndService | Sum charges; group by Customer, ServiceType, Month, Year |
| SortByTotalChargeDesc | Sort output by GrandTotalCharge descending |

### DF_InvoiceSummary_Transform
| Step | Operation |
|---|---|
| FilterActiveInvoices | Keep only PENDING and OVERDUE invoices |
| CastInvoiceTypes | Parse date and monetary strings to typed values |
| DeriveAgingAndFlags | Compute DaysOutstanding, AgingBucket (Current/1-30/31-60/61-90/90+), IsOverdue |
| JoinCustomerInfo | Inner join with customer_accounts on CustomerID |
| AggregateCustomerSummary | Sum amounts; count invoices by aging bucket per customer |
| SelectFinalColumns | Rename and select final output columns only |

---

## Trigger
The **TR_UPS_Finance_Daily_Schedule** trigger runs daily at **02:00 AM UTC**
and passes the previous day's date automatically as parameters to the master pipeline.

> To activate it: Go to **Manage → Triggers**, select the trigger, click **Start**.

---

## Notes
- All file names include the run date via ADF expressions (e.g. `billing_report_20240115.csv`)
- Each pipeline validates source file existence before executing the data flow
- Both child pipelines run **in parallel** under the master pipeline
- Integration Runtime assignment must be done manually after import
