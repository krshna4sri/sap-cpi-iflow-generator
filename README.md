# sap-cpi-iflow-generator
# SAP CPI iFlow Generator

A Streamlit-based tool to dynamically generate SAP CPI Integration Flows (iFlows) using reusable templates.

## Features

- Generate iFlows for:
  - GET
  - CREATE
  - UPDATE
  - DELETE

- Clone-based approach for safe reuse
- Supports:
  - Dynamic iFlow naming
  - Artifact ID control
  - Sender path update (GET)
  - Entity update (GET)

## Run Locally

```bash
pip install -r requirements.txt
streamlit run app.py
