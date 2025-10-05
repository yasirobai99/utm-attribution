# Marketing Attribution Analytics Dashboard

## Overview

This project builds a data pipeline and business intelligence dashboard for marketing attribution analysis using PostgreSQL for data storage and Power BI for visual analytics. It covers key attribution models like first-touch, last-touch, and position-based (U-shaped).

## Features

- Ingests and processes marketing event and ad spend data into PostgreSQL
- Implements SQL modeling for multiple attribution models
- Provides Power BI dashboards for conversion, CPA, CAC, and ROAS metrics
- Interactive visualizations with filters by date, source, medium, and campaign

## Data Model

- `mart.v_roi`: Aggregated data with cost, conversions, customers, revenue by campaign and date
- `mart.v_attribution`: Attribution credits by campaign and model
- Raw tables: web events and ad spend data loaded into staging

## Power BI Dashboard Pages

- **Overview:** KPIs for conversions, CPA, CAC, ROAS; CPA by campaign bar chart; ROAS over time line chart
- **Attribution Comparison:** Matrix view comparing attribution models side by side
- **Spend Efficiency:** Scatter plot of cost vs revenue weighted by conversions

## Setup Instructions

1. **PostgreSQL Setup**
   - Install PostgreSQL 16 or later.
   - Run `sql/create_tables.sql` to create schema and raw tables.
   - Run `python/load_postgres.py` to load raw CSV data.
   - Run `sql/build_models.sql` and `sql/powerbi_views.sql` to create modeling tables and views.

2. **Power BI Setup**
   - Connect Power BI Desktop to the PostgreSQL database (server: localhost, database: your_db).
   - Load views `mart.v_roi` and `mart.v_attribution`.
   - Create DAX measures as specified in code snippets.
   - Build visuals: cards, column charts, matrix, slicers as per dashboard layout.

3. **Refresh and Use**
   - Refresh data in Power BI to pull latest from PostgreSQL.
   - Interact with slicers and filters to analyze campaign performance and attribution.

## Technologies

- PostgreSQL 16+
- Python 3.10+ for data loading scripts
- Power BI Desktop for dashboarding
- SQL for table and view modeling

## References

- Kaggle UTM Attribution Dataset
- Power BI Official Documentation
- PostgreSQL Official Documentation

## License

This project is licensed under MIT License.
