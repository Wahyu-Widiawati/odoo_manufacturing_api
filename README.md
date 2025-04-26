# 🏭 Odoo Manufacturing Order API

This project automates the flow of manufacturing order data from **Google Sheets** to **Odoo ERP** using a two-stage pipeline:

1. Extract and transform data from Google Sheets to a PostgreSQL database using **R**.
2. Use **Python** to create Manufacturing Orders (MRPs) in **Odoo** via XML-RPC API.

> This is a portfolio project. All sensitive data (such as credentials or business logic) has been removed or replaced for public sharing.

---

## 🔧 Tech Stack

- **R**: Data extraction and transformation from Google Sheets
- **PostgreSQL**: Temporary storage layer
- **Python**: API interaction with Odoo ERP
- **Odoo (v17)**: Target ERP system using XML-RPC

---

## 🚀 How it Works

flowchart TD
    GSheet[Google Sheets Files] --> RScript[extract_gsheet_to_postgre.R]
    RScript --> Postgres[(PostgreSQL)]
    Postgres --> PyScript[postgre_to_odoo_api.py]
    PyScript --> Odoo[Odoo API (MRP)]
