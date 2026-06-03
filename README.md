# Weather Streaming Pipeline — Barranquilla, Colombia

A real-time weather data streaming pipeline built on Microsoft Azure and Microsoft Fabric that continuously ingests, stores, and visualizes meteorological and air quality data for Barranquilla, Colombia, with automated alerting capabilities.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Technology Stack](#3-technology-stack)
4. [Data Flow](#4-data-flow)
5. [Component Reference](#5-component-reference)
6. [Data Schema](#6-data-schema)
7. [Security](#7-security)
8. [CI/CD Pipeline](#8-cicd-pipeline)
9. [Monitoring & Alerting](#9-monitoring--alerting)
10. [Power BI Report](#10-power-bi-report)
11. [Repository Structure](#11-repository-structure)

---

## 1. Project Overview

### Objective

This project implements an end-to-end, near-real-time data engineering pipeline that polls the [WeatherAPI](https://www.weatherapi.com/) every **30 seconds** and streams the enriched payload — including current conditions, air quality indexes, weather alerts, and a 3-day forecast — through Azure Event Hub into a Microsoft Fabric KQL (Kusto) Database. A Power BI report delivers live dashboards, while a Fabric Reflex automation dispatches email alerts whenever a weather advisory is detected.

### Problem Statement

Weather events on the Caribbean coast of Colombia — high UV indexes, tropical storms, air quality spikes — require near-real-time awareness. Traditional batch pipelines introduce minutes or hours of latency between a condition change and its visibility in a dashboard. This pipeline closes that gap: data flows from the external API to a queryable analytical store in under 5 seconds, enabling stakeholders to react to weather conditions as they happen.

### Key Capabilities

| Capability | Detail |
|---|---|
| Ingestion cadence | Every 30 seconds via Azure Function App timer trigger |
| Data enrichment | Current weather + Air Quality Index (AQI) + Alerts + 3-day Forecast merged in a single event |
| Streaming broker | Azure Event Hub |
| Real-time store | Microsoft Fabric Eventhouse / KQL Database |
| Alerting | Microsoft Fabric Reflex (Data Activator) — email on new weather alerts |
| Visualization | Microsoft Fabric Power BI Report (DirectLake / Real-Time) |
| Secret management | Azure Key Vault + Managed Identity (no hardcoded credentials) |
| Source control & CI/CD | Azure DevOps with automatic GitHub mirror |

---

## 2. Architecture

### Architecture Diagram

![Architecture Diagram](assets/Architecture%20Diagram.png)

### Architecture Pattern

This pipeline follows the **Kappa** architecture pattern — a single streaming path with no separate batch layer. All historical and real-time queries are served from the same KQL Database, which is optimized for time-series analytics and supports sub-second query latency over millions of rows.

The **DEV / TESTING** layer (Azure Databricks) is shown separately from the production path. Databricks was used during the development phase to prototype and validate the pipeline end-to-end before transitioning to Azure Functions for production. It is not part of the live data flow.

### Fabric Workspace

All Microsoft Fabric artifacts — Eventhouse, Eventstream, Power BI Report, Semantic Model, and Reflex — are deployed under a single workspace, keeping the analytical layer fully contained within the Microsoft Fabric ecosystem.

![Fabric Workspace](assets/Fabric%20workspace.png)

---

## 3. Technology Stack

| Layer | Service / Tool | Purpose |
|---|---|---|
| External Data Source | [WeatherAPI.com](https://www.weatherapi.com/) | REST API — current weather, AQI, alerts, forecast |
| Ingestion | Azure Function App (Python 3.x) | Serverless timer trigger, scales to zero when idle |
| Secret Management | Azure Key Vault | Centralized secret storage, no credentials in code |
| Identity & Access | Azure Managed Identity (`DefaultAzureCredential`) | Passwordless authentication to Azure services |
| Streaming Broker | Azure Event Hub | Fully managed real-time event ingestion |
| Stream Routing | Microsoft Fabric Eventstream | No-code connector from Event Hub to Eventhouse |
| Analytical Store | Microsoft Fabric Eventhouse (KQL Database) | Time-series store, Kusto Query Language |
| Alerting | Microsoft Fabric Reflex (Data Activator) | Event-driven rules and email notifications |
| Visualization | Microsoft Fabric Power BI | Real-time dashboards via DirectLake |
| CI/CD | Azure DevOps Pipelines | Automatic GitHub mirror on every commit |
| Source Control | Azure DevOps Repos + GitHub (mirror) | Version control |

### Why Azure Functions over Databricks for production ingestion

Databricks was evaluated and used during development. The decision to move to Azure Functions for production came down to two factors:

- **Cost:** A Databricks cluster must remain active to process a continuous stream, incurring compute charges 24/7 regardless of data volume. For a pipeline that only polls an external API and forwards the result, this overhead is unjustifiable.
- **Fit for purpose:** This ingestion task requires no complex transformations, joins, or distributed processing. Azure Functions is event-driven, scales to zero between executions, and includes **1 million free executions per month** on any Azure subscription — making the ingestion layer effectively free at a 30-second polling cadence.

---

## 4. Data Flow

Every 30 seconds, the following sequence executes end-to-end:

```
 t = 0s   Azure Function App timer fires
           │
           ▼
 t = 0s   Function retrieves API key from Azure Key Vault via Managed Identity
           │
           ▼
 t ≈ 1s   Three REST calls to WeatherAPI.com:
           ├── GET /current.json  (temperature, wind, humidity, AQI)
           ├── GET /forecast.json (3-day forecast)
           └── GET /alerts.json  (active weather advisories)
           │
           ▼
 t ≈ 2s   Responses flattened and merged into a single JSON document
           │
           ▼
 t ≈ 2s   JSON published as an EventData batch to Azure Event Hub
           │
           ▼
 t ≈ 3s   Fabric Eventstream reads the event from Event Hub ($Default consumer group)
           │
           ▼
 t ≈ 4s   Event written to KQL table (weather-table-bq) in the Eventhouse
           │
           ▼
 t ≈ 4s   Power BI Report reflects the new data point
           │
           ▼
 t = 60s  Reflex evaluates the alert detection KQL query:
           ├── New alert found → email dispatched immediately
           └── No new alerts   → no action
```

**End-to-end latency:** under 5 seconds from API call to data available in the KQL Database.

The screenshot below shows a real event payload in the Azure Event Hub Data Explorer — the full flattened JSON document with weather, air quality, alerts, and forecast visible as a single message.

![Event Hub Data Explorer](assets/Event%20Hub%20-%20Data%20Explorer.png)

---

## 5. Component Reference

### 5.1 Azure Function App

The production ingestion component. A Python timer-triggered function (`weatherapifunction`) fires every 30 seconds, fetches data from three WeatherAPI endpoints, merges the responses into a flat JSON document, and publishes the result to Azure Event Hub.

Authentication is handled entirely through `DefaultAzureCredential`, which resolves to the Function App's System-Assigned Managed Identity in Azure — no connection strings or API keys are ever stored in the code or environment variables.

The Function App `fp-weather-streaming01` runs on Linux (East US 2) with a Flex Consumption plan.

![Azure Function App Overview](assets/Azure%20Fuction%20Overview.png)

![Function App Code](assets/weatherapifunction%20code.png)

### 5.2 Databricks Notebook (Development)

Used during development to prototype and validate the pipeline incrementally — from a simple API call, to Event Hub connectivity, to full streaming via Spark Structured Streaming. Not part of the production flow. Retained in the repository as a development artifact.

### 5.3 Microsoft Fabric Eventstream

A no-code stream processor that connects Azure Event Hub as a source and routes events directly to the Eventhouse as a destination, with no transformation logic required. The canvas below shows the complete topology in **Active** state with live data previewed at the bottom.

![Eventstream Canvas](assets/Event%20Stream.png)

### 5.4 Microsoft Fabric Eventhouse / KQL Database

The analytical store for all ingested weather data. Built on Azure Data Explorer (Kusto) embedded in Microsoft Fabric, it supports sub-second analytical queries over the full historical dataset and serves as the single source of truth for both the Power BI report and the Reflex alerting rule.

- **Cluster URI:** `https://trd-5uxdtybf2f09n783bs.z2.kusto.fabric.microsoft.com`
- **Database:** `weather-eventhouse-bq`
- **Table:** `weather-table-bq`

![Eventhouse KQL Database](assets/Event%20House.png)

### 5.5 Reflex (Data Activator)

An event-driven automation rule that runs a KQL query every 60 seconds against the Eventhouse. When a new weather alert is detected — one not seen in the previous minute — it dispatches an email notification with the alert details.

The deduplication logic uses a left-anti join to ensure each unique alert triggers exactly one email, regardless of how many times the same condition appears in the ingestion window.

**Alert detection query:**

```kql
['weather-table-bq']
| where alerts != '[]'
| extend AlertValue = tostring(alerts)
| summarize LastTriggered = max(EventProcessedUtcTime) by AlertValue
| join kind=leftanti (
    ['weather-table-bq']
    | where alerts != '[]'
    | extend AlertValue = tostring(alerts)
    | summarize LastTriggered = max(EventProcessedUtcTime) by AlertValue
    | where LastTriggered < ago(1m)
) on AlertValue
```

![Alert Detection KQL Query](assets/Alert%20Code.png)

![Weather Alerts Reflex Rule](assets/Weather%20Alerts.png)

---

## 6. Data Schema

Each event published to Event Hub and stored in the KQL table carries the following structure:

### KQL Table Schema (`weather-table-bq`)

| Column | Type | Description |
|---|---|---|
| `name` | string | City name |
| `region` | string | Region / department |
| `country` | string | Country |
| `lat` / `lon` | real | Geographic coordinates |
| `localtime` | string | Local timestamp at location |
| `temp_c` | real | Temperature in Celsius |
| `is_day` | long | 1 = daytime, 0 = nighttime |
| `condition_text` | string | Human-readable weather condition |
| `wind_kph` | real | Wind speed (km/h) |
| `wind_degree` / `wind_dir` | long / string | Wind direction |
| `pressure_in` | real | Atmospheric pressure (inches Hg) |
| `precip_in` | real | Precipitation (inches) |
| `humidity` | long | Relative humidity (%) |
| `cloud` | long | Cloud cover (%) |
| `feelslike_c` | real | Feels-like temperature (°C) |
| `uv` | real | UV index |
| `air_quality` | dynamic | CO, NO₂, O₃, SO₂, PM2.5, PM10, EPA index, DEFRA index |
| `alerts` | dynamic | Array of active weather advisories |
| `forecast` | dynamic | 3-day forecast array (date, max/min temp, condition) |
| `EventProcessedUtcTime` | datetime | UTC timestamp — event processed by Fabric |
| `PartitionId` | long | Event Hub partition |
| `EventEnqueuedUtcTime` | datetime | UTC timestamp — event enqueued in Event Hub |

### Air Quality Index Reference

| Field | Pollutant | Unit |
|---|---|---|
| `co` | Carbon Monoxide | μg/m³ |
| `no2` | Nitrogen Dioxide | μg/m³ |
| `o3` | Ozone | μg/m³ |
| `so2` | Sulphur Dioxide | μg/m³ |
| `pm2_5` | Particulate Matter < 2.5μm | μg/m³ |
| `pm10` | Particulate Matter < 10μm | μg/m³ |
| `us-epa-index` | US EPA AQI category | 1 (Good) – 6 (Hazardous) |
| `gb-defra-index` | UK DEFRA index | 1 (Low) – 10 (Very High) |

---

## 7. Security

All security controls in this project follow the principle of **zero hardcoded credentials**.

| Control | Implementation |
|---|---|
| No credentials in source code | All secrets stored in Azure Key Vault; `local.settings.json` excluded via `.gitignore` |
| Passwordless authentication | Function App uses System-Assigned Managed Identity with `DefaultAzureCredential` |
| Least privilege | Managed Identity holds only `Key Vault Secrets User` and `Event Hubs Data Sender` roles |
| Transport encryption | All connections use TLS 1.2+ (HTTPS to WeatherAPI, AMQP-over-TLS to Event Hub) |
| Secret rotation | Rotating any secret in Key Vault takes effect on the next function execution — no redeployment needed |

Both secrets (`weatherapikey` and `eventhub-connection-string`) are stored in `kv-weather-streaming01` with **Enabled** status. Their values are never exposed — only names and metadata are visible.

![Key Vault Secrets](assets/Key%20Vault%20Secrets.png)

---

## 8. CI/CD Pipeline

The repository is hosted in **Azure DevOps Repos**. Every commit to `main` triggers an `azure-pipelines.yml` pipeline that automatically mirrors the codebase to the public GitHub repository [`Jeancmn/weather-streaming-bq-proyect`](https://github.com/Jeancmn/weather-streaming-bq-proyect).

```yaml
trigger:
  branches:
    include:
      - main

pool:
  vmImage: 'ubuntu-latest'

steps:
- checkout: self
  persistCredentials: true
  fetchDepth: 0

- script: |
    git config --global user.email "$(GIT_USER_EMAIL)"
    git config --global user.name "Azure DevOps Pipeline"
    git remote add github https://$(GITHUB_TOKEN)@github.com/Jeancmn/weather-streaming-bq-proyect.git
    git push github HEAD:refs/heads/main --force
  displayName: 'Sync mirror to GitHub'
  env:
    GITHUB_TOKEN: $(GITHUB_TOKEN)
```

The `GITHUB_TOKEN` and `GIT_USER_EMAIL` are stored as secret pipeline variables — not exposed in the YAML file.

![Azure DevOps Pipeline Runs](assets/Azure%20Devops%20pipeline.png)

![Azure DevOps Repository](assets/Azure%20Devops%20Repo.png)

---

## 9. Monitoring & Alerting

### Function App

The Function App is connected to Azure Application Insights via `host.json`, which captures execution logs, failures, and performance metrics for every timer invocation.

### Weather Alert Notifications

The **Weather Alerts.Reflex** rule runs every **60 seconds** inside Microsoft Fabric. When the KQL alert query returns new rows, it sends an email with:

- **Subject:** `Weather Alert`
- **Headline:** `A weather alert has been received in Barranquilla`
- **Context fields:** `AlertValue` (full alert JSON), `LastTriggered` (timestamp)

This alerting mechanism runs entirely within Fabric — no external notification infrastructure is required.

---

## 10. Power BI Report

The Fabric Workspace contains a two-page Power BI report connected to the KQL Semantic Model via **DirectLake** mode, which reads data directly from the Eventhouse without import — providing near-real-time refresh latency.

### Semantic Model Tables

| Table | Description |
|---|---|
| `history_weather_data` | Full historical time series — all ingested records |
| `Latest_weather_data` | Most recent observation |

### Page 1 — Live Conditions

Real-time snapshot of current meteorological conditions: temperature, feels-like temperature, wind speed and direction, humidity, UV index, and sky condition.

![Live Conditions](assets/Live%20Conditions.png)

### Page 2 — Air Quality & Atmosphere

Atmospheric and air quality indicators: CO, NO₂, O₃, SO₂, PM2.5, PM10, pressure, precipitation, and cloud cover — including EPA and DEFRA indexes.

![Air Quality & Atmosphere](assets/Air%20Quality%20%26%20Atmosphere.png)

---

## 11. Repository Structure

```
weather-streaming-bq/
│
├── azure-pipelines.yml                         # CI/CD — mirrors main branch to GitHub on every commit
├── README.md
│
├── weather-streaming-function-app/             # Production ingestion — Azure Function App (Python)
│   ├── function_app.py                         # Timer trigger: fetches weather data, publishes to Event Hub
│   ├── host.json                               # Functions runtime config (v2, Extension Bundle 4.x)
│   ├── local.settings.json                     # Local dev settings (not committed)
│   └── requirements.txt                        # Python dependencies
│
├── databricks-weather-streaming-notebooks/     # Development & testing only — not production
│   └── weather-streaming-notebook.py           # Iterative prototype: API → Event Hub → streaming
│
├── fabric-weather-streaming-ws/                # Microsoft Fabric Workspace artifacts
│   ├── for-alerts.KQLQueryset/                 # KQL alert detection query
│   ├── Weather Alerts.Reflex/                  # Reflex rule — email on new weather alerts
│   ├── weather-eventhouse-bq.Eventhouse/       # Eventhouse + KQL Database (schema, properties)
│   ├── weather-eventstream-bq.Eventstream/     # Eventstream topology (Event Hub → Eventhouse)
│   ├── Weather-Report-Barranquilla-Colombia.Report/     # Power BI Report definition
│   └── Weather-Report-Barranquilla-Colombia.SemanticModel/  # Semantic Model (tables, relationships)
│
└── assets/                                     # Screenshots and diagrams used in this README
```

---

## Authors

| Name | Role |
|---|---|
| Jean Carlos Mangones | Data Engineer |

---

*Last updated: June 2026*
