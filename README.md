# Weather Streaming Pipeline — Barranquilla, Colombia

A real-time weather data streaming pipeline built on Microsoft Azure and Microsoft Fabric that continuously ingests, stores, and visualizes meteorological and air quality data for Barranquilla, Colombia, with automated alerting capabilities.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Technology Stack](#3-technology-stack)
4. [Prerequisites](#4-prerequisites)
5. [Azure Infrastructure Setup](#5-azure-infrastructure-setup)
6. [Data Flow](#6-data-flow)
7. [Component Reference](#7-component-reference)
8. [Data Schema](#8-data-schema)
9. [Configuration & Secrets Management](#9-configuration--secrets-management)
10. [Deployment](#10-deployment)
11. [CI/CD Pipeline](#11-cicd-pipeline)
12. [Monitoring & Alerting](#12-monitoring--alerting)
13. [Power BI Report](#13-power-bi-report)
14. [Security Considerations](#14-security-considerations)
15. [Troubleshooting](#15-troubleshooting)
16. [Repository Structure](#16-repository-structure)

---

## 1. Project Overview

### Objective

This project implements an end-to-end, near-real-time data engineering pipeline that polls the [WeatherAPI](https://www.weatherapi.com/) every **30 seconds** and streams the enriched payload — including current conditions, air quality indexes, weather alerts, and a 3-day forecast — through Azure Event Hub into a Microsoft Fabric KQL (Kusto) Database. A Power BI report delivers live dashboards, while a Fabric Reflex automation dispatches email alerts whenever a weather advisory is detected.

### Business Problem

Weather events in the Caribbean coast of Colombia (high UV index, tropical storms, air quality spikes) require near-real-time awareness. Polling an external API on a fixed cadence, shipping the data through a managed streaming broker, and surfacing it in a low-latency analytical store allows stakeholders to react within seconds of a condition change — a capability that traditional batch pipelines cannot provide.

### Key Capabilities

| Capability | Detail |
|---|---|
| Ingestion cadence | Every 30 seconds via Azure Function App timer trigger |
| Data enrichment | Current weather + Air Quality Index (AQI) + Alerts + 3-day Forecast merged in a single event |
| Streaming broker | Azure Event Hub (Standard tier) |
| Real-time store | Microsoft Fabric Eventhouse / KQL Database |
| Alerting | Microsoft Fabric Reflex (Data Activator) — email on new weather alerts |
| Visualization | Microsoft Fabric Power BI Report (DirectLake / Real-Time) |
| Secret management | Azure Key Vault + Managed Identity (no hardcoded credentials) |
| Source control & CI/CD | Azure DevOps with automatic GitHub mirror |

---

## 2. Architecture

### High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCE LAYER                                  │
│                                                                                 │
│          ┌──────────────────────────────────────────────────────┐               │
│          │              WeatherAPI.com (REST)                   │               │
│          │  /current.json  /forecast.json  /alerts.json        │               │
│          └──────────────────────┬───────────────────────────────┘               │
└─────────────────────────────────┼───────────────────────────────────────────────┘
                                  │ HTTPS / JSON (every 30s)
┌─────────────────────────────────▼───────────────────────────────────────────────┐
│                            INGESTION LAYER                                      │
│                                                                                 │
│   ┌──────────────────────────────────────────────────────────────────────────┐  │
│   │               Azure Function App  (Python 3.x — Timer Trigger)          │  │
│   │                                                                          │  │
│   │   • Reads API key from Azure Key Vault (Managed Identity)               │  │
│   │   • Calls 3 WeatherAPI endpoints in sequence                            │  │
│   │   • Flattens & merges responses into a single JSON payload              │  │
│   │   • Publishes event to Azure Event Hub                                  │  │
│   │   • Monitored via Application Insights                                  │  │
│   └──────────────────────────────────────┬───────────────────────────────────┘  │
│                                          │                                       │
│   ┌──────────────────────────────────────▼───────────────────────────────────┐  │
│   │                     Azure Key Vault (kv-weather-streaming01)             │  │
│   │   Secrets: weatherapikey  |  eventhub-connection-string                 │  │
│   └──────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────┬───────────────────────────────────────────────┘
                                  │ AMQP / JSON (EventData batch)
┌─────────────────────────────────▼───────────────────────────────────────────────┐
│                            STREAMING LAYER                                      │
│                                                                                 │
│   ┌──────────────────────────────────────────────────────────────────────────┐  │
│   │   Azure Event Hub  (weatherstreamingevenhub)                            │  │
│   │   Namespace: weatherstreamingnamespace0.servicebus.windows.net          │  │
│   │   Consumer Group: $Default                                              │  │
│   └──────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────┬───────────────────────────────────────────────┘
                                  │ Fabric Eventstream connector
┌─────────────────────────────────▼───────────────────────────────────────────────┐
│                        MICROSOFT FABRIC WORKSPACE                               │
│                                                                                 │
│   ┌──────────────────────────────────────────────────────────────────────────┐  │
│   │        Eventstream  (weather-eventstream-bq)                            │  │
│   │        Source: AzureEventHub → Stream → Destination: Eventhouse         │  │
│   └──────────────────────────────────────────────────────────────────────────┘  │
│                                          │                                       │
│   ┌──────────────────────────────────────▼───────────────────────────────────┐  │
│   │        Eventhouse / KQL Database  (weather-eventhouse-bq)               │  │
│   │        Table: weather-table-bq                                          │  │
│   │        Engine: Azure Data Explorer (Kusto)                              │  │
│   └───────────────────┬──────────────────────────────────────┬──────────────┘  │
│                        │                                      │                  │
│   ┌────────────────────▼────────────────┐  ┌────────────────▼──────────────┐   │
│   │   Power BI Report                  │  │  Reflex (Data Activator)      │   │
│   │   Weather-Report-Barranquilla-     │  │  Rule: new alert in KQL DB    │   │
│   │   Colombia.Report                  │  │  Action: Email notification   │   │
│   │   (Real-Time / DirectLake)         │  │  Cadence: every 60 seconds    │   │
│   └────────────────────────────────────┘  └───────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Architecture Pattern

This pipeline follows the **Lambda-lite / Kappa** pattern — there is a single streaming path with no separate batch layer. All historical and real-time queries are served by the same KQL Database, which is optimized for time-series analytics and supports sub-second query latency over millions of rows.

---

## 3. Technology Stack

| Layer | Service / Tool | Purpose |
|---|---|---|
| External Data Source | [WeatherAPI.com](https://www.weatherapi.com/) | REST API providing weather, AQI, alerts, forecast |
| Ingestion / Orchestration | Azure Function App (Python 3.x) | Serverless compute, timer trigger every 30 s |
| Secret Management | Azure Key Vault | Secure storage of API keys and connection strings |
| Identity & Access | Azure Managed Identity (`DefaultAzureCredential`) | Passwordless authentication to Azure services |
| Streaming Broker | Azure Event Hub | Fully managed real-time event ingestion at scale |
| Stream Processing | Microsoft Fabric Eventstream | No-code stream routing from Event Hub to Eventhouse |
| Analytical Store | Microsoft Fabric Eventhouse (KQL Database) | Time-series store with Kusto Query Language |
| Alerting Automation | Microsoft Fabric Reflex (Data Activator) | Event-driven rules and email notifications |
| Visualization | Microsoft Fabric Power BI (Report + Semantic Model) | Interactive real-time dashboards |
| Monitoring | Azure Application Insights | Function App telemetry and logging |
| CI/CD | Azure DevOps Pipelines | Build + GitHub mirror synchronization |
| Source Control | Azure DevOps Repos + GitHub (mirror) | Version control |

---

## 4. Prerequisites

### Azure Subscriptions & Permissions

- An active **Azure subscription** with `Contributor` or `Owner` role on the target resource group.
- A **Microsoft Fabric** capacity (F2 or higher recommended for real-time workloads).
- Access to [WeatherAPI.com](https://www.weatherapi.com/) — a free-tier API key is sufficient for single-location polling.

### Required Azure Resources (provision before deployment)

| Resource | Name | Notes |
|---|---|---|
| Resource Group | (your choice) | Contains all Azure resources |
| Azure Function App | (your choice) | Python 3.x, Consumption or Premium plan |
| Azure Event Hub Namespace | `weatherstreamingnamespace0` | Standard tier minimum |
| Azure Event Hub | `weatherstreamingevenhub` | Inside the namespace above |
| Azure Key Vault | `kv-weather-streaming01` | RBAC-enabled preferred |
| Application Insights | (linked to Function App) | For monitoring |
| Microsoft Fabric Workspace | (your choice) | Contains Eventhouse, Eventstream, Report, Reflex |

### Local Development Tools

```
Python          3.9+
Azure Functions Core Tools  v4
Azure CLI       2.50+
Git             2.x
```

---

## 5. Azure Infrastructure Setup

Follow these steps in order. Each step must be completed before proceeding to the next.

### Step 1 — Create the Resource Group

```bash
az group create \
  --name rg-weather-streaming \
  --location eastus
```

### Step 2 — Provision Azure Key Vault

```bash
az keyvault create \
  --name kv-weather-streaming01 \
  --resource-group rg-weather-streaming \
  --location eastus \
  --enable-rbac-authorization true
```

### Step 3 — Provision Azure Event Hub Namespace and Event Hub

```bash
# Create namespace
az eventhubs namespace create \
  --name weatherstreamingnamespace0 \
  --resource-group rg-weather-streaming \
  --location eastus \
  --sku Standard

# Create Event Hub inside the namespace
az eventhubs eventhub create \
  --name weatherstreamingevenhub \
  --resource-group rg-weather-streaming \
  --namespace-name weatherstreamingnamespace0 \
  --message-retention 1 \
  --partition-count 2
```

### Step 4 — Deploy the Azure Function App

```bash
az functionapp create \
  --name <your-function-app-name> \
  --resource-group rg-weather-streaming \
  --storage-account <your-storage-account> \
  --consumption-plan-location eastus \
  --runtime python \
  --runtime-version 3.11 \
  --functions-version 4 \
  --assign-identity '[system]'
```

> The `--assign-identity '[system]'` flag enables a **System-Assigned Managed Identity** on the Function App. This identity is used for passwordless authentication to Key Vault and Event Hub.

### Step 5 — Grant Managed Identity Permissions

Replace `<function-app-principal-id>` with the Object ID of the system-assigned identity (visible in the Azure Portal under Function App → Identity).

```bash
# Key Vault: allow the Function App to read secrets
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee <function-app-principal-id> \
  --scope /subscriptions/<sub-id>/resourceGroups/rg-weather-streaming/providers/Microsoft.KeyVault/vaults/kv-weather-streaming01

# Event Hub: allow the Function App to send events
az role assignment create \
  --role "Azure Event Hubs Data Sender" \
  --assignee <function-app-principal-id> \
  --scope /subscriptions/<sub-id>/resourceGroups/rg-weather-streaming/providers/Microsoft.EventHub/namespaces/weatherstreamingnamespace0
```

### Step 6 — Store Secrets in Key Vault

```bash
# WeatherAPI key
az keyvault secret set \
  --vault-name kv-weather-streaming01 \
  --name weatherapikey \
  --value "<your-weatherapi-key>"

# Event Hub connection string (used by Databricks notebook only)
az keyvault secret set \
  --vault-name kv-weather-streaming01 \
  --name eventhub-connection-string \
  --value "<your-event-hub-connection-string>"
```

### Step 7 — Create the Microsoft Fabric Workspace

1. Navigate to [Microsoft Fabric](https://app.fabric.microsoft.com).
2. Create a new Workspace and assign it to a Fabric capacity.
3. Import the items from the `fabric-weather-streaming-ws/` folder using **Fabric Git Integration** or the Fabric REST API.

### Step 8 — Configure the Eventstream

Inside the Fabric Workspace:

1. Open **weather-eventstream-bq**.
2. In the source node, connect to the Azure Event Hub:
   - Namespace: `weatherstreamingnamespace0`
   - Event Hub: `weatherstreamingevenhub`
   - Consumer Group: `$Default`
   - Authentication: Connection string (using the `eventhub-connection-string` secret)
3. In the destination node, point to **weather-eventhouse-bq** → database **weather-eventhouse-bq** → table **weather-table-bq**.
4. Activate the Eventstream.

### Step 9 — Create the KQL Table

Open the **KQL Queryset** in the Fabric Workspace and run the following schema creation command:

```kql
.create-merge table ['weather-table-bq'] (
    name: string,
    region: string,
    country: string,
    lat: real,
    lon: real,
    localtime: string,
    temp_c: real,
    is_day: long,
    condition_text: string,
    condition_icon: string,
    wind_kph: real,
    wind_degree: long,
    wind_dir: string,
    pressure_in: real,
    precip_in: real,
    humidity: long,
    cloud: long,
    feelslike_c: real,
    uv: real,
    air_quality: dynamic,
    alerts: dynamic,
    forecast: dynamic,
    EventProcessedUtcTime: datetime,
    PartitionId: long,
    EventEnqueuedUtcTime: datetime
)
```

### Step 10 — Configure the Reflex Alert

1. Open **Weather Alerts.Reflex** in the Fabric Workspace.
2. Verify the KQL source query is pointing to `weather-eventhouse-bq`.
3. Update the email recipient in the **Act** step if needed.
4. Enable the rule (set `shouldRun: true`).

---

## 6. Data Flow

The following sequence describes what happens from data origin to dashboard on every 30-second cycle:

```
 t = 0s   Azure Function App timer fires
           │
           ▼
 t = 0s   Function reads WeatherAPI key from Azure Key Vault
           │
           ▼
 t ≈ 1s   Three parallel REST calls to WeatherAPI.com:
           ├── GET /current.json?q=Barranquilla&aqi=yes
           ├── GET /forecast.json?q=Barranquilla&days=3
           └── GET /alerts.json?q=Barranquilla&alerts=yes
           │
           ▼
 t ≈ 2s   Flatten & merge all three responses into a single JSON document
           │
           ▼
 t ≈ 2s   Publish JSON document as EventData batch to Azure Event Hub
           │
           ▼
 t ≈ 3s   Fabric Eventstream picks up the event from Event Hub ($Default consumer group)
           │
           ▼
 t ≈ 4s   Eventstream writes the event to the KQL table (weather-table-bq)
           Metadata columns appended: EventProcessedUtcTime, PartitionId, EventEnqueuedUtcTime
           │
           ▼
 t ≈ 4s   Power BI Report auto-refreshes — new data point visible in dashboards
           │
           ▼
 t = 60s  Reflex rule evaluates the KQL query:
           ├── New alert detected → Email sent to stakeholder
           └── No new alerts    → No action
```

**End-to-end latency:** typically under 5 seconds from API call to data available in the KQL Database.

---

## 7. Component Reference

### 7.1 Azure Function App (`weather-streaming-function-app/`)

| File | Description |
|---|---|
| `function_app.py` | Main function entry point — timer trigger, data fetch, Event Hub publish |
| `host.json` | Azure Functions runtime configuration (v2, Extension Bundle 4.x) |
| `local.settings.json` | Local development environment variables (not committed to production) |
| `requirements.txt` | Python package dependencies |

**Timer schedule:** `*/30 * * * * *` — fires every 30 seconds.

**Authentication:** Uses `DefaultAzureCredential`, which resolves to the System-Assigned Managed Identity in Azure and to the local developer's `az login` session during development.

**Python dependencies:**

```
azure-functions
azure-eventhub
azure-identity
azure-keyvault-secrets
requests
```

### 7.2 Databricks Notebook (`databricks-weather-streaming-notebooks/`)

The notebook `weather-streaming-notebook.py` was used during **development and testing** of the pipeline. It documents the incremental approach taken to build the solution:

1. **Test Event Hub connectivity** — sends a minimal test event to validate the connection string.
2. **Test WeatherAPI integration** — validates the API key and response structure.
3. **Full payload assembly** — merges current weather, forecast, and alerts.
4. **Streaming with Spark Structured Streaming** — uses `spark.readStream` (rate source) with `foreachBatch` to send events continuously.
5. **Throttled streaming** — sends one event every 30 seconds using a time-delta guard inside `foreachBatch`.

> **Note:** The Databricks notebook is not used in production. The Azure Function App (`weather-streaming-function-app/`) is the production ingestion component. The notebook is retained as reference and for local testing without deploying the Function App.

### 7.3 Microsoft Fabric Eventstream (`weather-eventstream-bq.Eventstream`)

| Property | Value |
|---|---|
| Source type | Azure Event Hub |
| Source name | `AzEventHub` |
| Consumer group | `$Default` |
| Input serialization | JSON / UTF-8 |
| Stream name | `weather-eventstream-bq-stream` |
| Destination type | Eventhouse |
| Ingestion mode | Processed Ingestion |
| Target table | `weather-table-bq` |

### 7.4 Microsoft Fabric Eventhouse / KQL Database (`weather-eventhouse-bq`)

- **Engine:** Azure Data Explorer (Kusto) embedded in Microsoft Fabric.
- **Cluster URI:** `https://trd-5uxdtybf2f09n783bs.z2.kusto.fabric.microsoft.com`
- **Database:** `weather-eventhouse-bq`
- **Primary table:** `weather-table-bq`

The KQL engine supports sub-second analytical queries over the entire historical dataset, making it suitable for both real-time dashboards and historical trend analysis.

### 7.5 Reflex (Data Activator) — `Weather Alerts.Reflex`

| Property | Value |
|---|---|
| Execution interval | 60 seconds |
| KQL source | `weather-eventhouse-bq` → `weather-table-bq` |
| Trigger condition | New rows where `alerts != '[]'` not seen in the last minute |
| Action | Email to the configured recipient — Subject: "Weather Alert" |
| Email body fields | `AlertValue`, `LastTriggered` |

**Alert detection KQL query:**

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

This query uses a left-anti join to return only alert values that appeared **within the last minute** and have not been seen before — effectively deduplicating repeated alerts and triggering the email exactly once per unique alert event.

---

## 8. Data Schema

### Event Payload (JSON — published to Event Hub)

```json
{
  "name": "Barranquilla",
  "region": "Atlantico",
  "country": "Colombia",
  "lat": 10.96,
  "lon": -74.8,
  "localtime": "2024-01-15 14:30",
  "temp_c": 32.0,
  "is_day": 1,
  "condition_text": "Sunny",
  "condition_icon": "//cdn.weatherapi.com/weather/64x64/day/113.png",
  "wind_kph": 22.0,
  "wind_degree": 30,
  "wind_dir": "NNE",
  "pressure_in": 29.8,
  "precip_in": 0.0,
  "humidity": 70,
  "cloud": 0,
  "feelslike_c": 38.5,
  "uv": 9.0,
  "air_quality": {
    "co": 255.0,
    "no2": 5.5,
    "o3": 55.0,
    "so2": 2.1,
    "pm2_5": 8.3,
    "pm10": 12.0,
    "us-epa-index": 1,
    "gb-defra-index": 1
  },
  "alerts": [
    {
      "headline": "Tropical Storm Warning",
      "severity": "Moderate",
      "description": "Tropical storm conditions expected...",
      "instruction": "Seek shelter immediately."
    }
  ],
  "forecast": [
    { "date": "2024-01-15", "maxtemp_c": 34.0, "mintemp_c": 27.0, "condition": "Sunny" },
    { "date": "2024-01-16", "maxtemp_c": 33.0, "mintemp_c": 26.0, "condition": "Partly cloudy" },
    { "date": "2024-01-17", "maxtemp_c": 31.0, "mintemp_c": 25.0, "condition": "Patchy rain possible" }
  ]
}
```

### KQL Table Schema (`weather-table-bq`)

| Column | Type | Source | Description |
|---|---|---|---|
| `name` | string | WeatherAPI | City name |
| `region` | string | WeatherAPI | Region / department |
| `country` | string | WeatherAPI | Country |
| `lat` | real | WeatherAPI | Latitude |
| `lon` | real | WeatherAPI | Longitude |
| `localtime` | string | WeatherAPI | Local timestamp at location |
| `temp_c` | real | WeatherAPI | Temperature in Celsius |
| `is_day` | long | WeatherAPI | 1 = daytime, 0 = nighttime |
| `condition_text` | string | WeatherAPI | Human-readable weather condition |
| `condition_icon` | string | WeatherAPI | URL to condition icon |
| `wind_kph` | real | WeatherAPI | Wind speed (km/h) |
| `wind_degree` | long | WeatherAPI | Wind direction in degrees |
| `wind_dir` | string | WeatherAPI | Wind direction abbreviation |
| `pressure_in` | real | WeatherAPI | Atmospheric pressure (inches Hg) |
| `precip_in` | real | WeatherAPI | Precipitation (inches) |
| `humidity` | long | WeatherAPI | Relative humidity (%) |
| `cloud` | long | WeatherAPI | Cloud cover (%) |
| `feelslike_c` | real | WeatherAPI | Feels-like temperature (°C) |
| `uv` | real | WeatherAPI | UV index |
| `air_quality` | dynamic | WeatherAPI | Nested object — CO, NO2, O3, SO2, PM2.5, PM10, EPA index, DEFRA index |
| `alerts` | dynamic | WeatherAPI | Array of active weather alerts |
| `forecast` | dynamic | WeatherAPI | Array of 3-day forecast objects |
| `EventProcessedUtcTime` | datetime | Event Hub / Fabric | UTC timestamp when event was processed |
| `PartitionId` | long | Event Hub | Event Hub partition identifier |
| `EventEnqueuedUtcTime` | datetime | Event Hub | UTC timestamp when event was enqueued |

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

## 9. Configuration & Secrets Management

### Key Vault Secrets

| Secret Name | Used By | Description |
|---|---|---|
| `weatherapikey` | Function App, Databricks Notebook | WeatherAPI.com API key |
| `eventhub-connection-string` | Databricks Notebook | Event Hub namespace connection string (for notebook testing only) |

### Function App — Important Configuration Values

| Setting | Value | Location |
|---|---|---|
| Event Hub Namespace | `weatherstreamingnamespace0.servicebus.windows.net` | `function_app.py` |
| Event Hub Name | `weatherstreamingevenhub` | `function_app.py` |
| Key Vault URL | `https://kv-weather-streaming01.vault.azure.net/` | `function_app.py` |
| Target Location | `Barranquilla` | `function_app.py` |
| Timer Schedule | `*/30 * * * * *` | `function_app.py` |

### Local Development (`local.settings.json`)

This file is excluded from source control via `.gitignore`. For local testing, set the following values:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AZURE_CLIENT_ID": "<your-service-principal-client-id>",
    "AZURE_TENANT_ID": "<your-tenant-id>",
    "AZURE_CLIENT_SECRET": "<your-service-principal-secret>"
  }
}
```

> When running locally, `DefaultAzureCredential` will pick up either the environment variables above or your active `az login` session. In Azure, it uses the Managed Identity automatically — no environment variables needed.

---

## 10. Deployment

### Deploy the Azure Function App

**Option A — Azure Functions Core Tools (recommended for development)**

```bash
cd weather-streaming-function-app

# Install dependencies
pip install -r requirements.txt

# Run locally
func start

# Deploy to Azure
func azure functionapp publish <your-function-app-name>
```

**Option B — Azure DevOps Pipeline (CI/CD)**

Any push to the `main` branch triggers the `azure-pipelines.yml` pipeline, which currently handles the GitHub mirror synchronization. To extend it with Function App deployment, add a deployment stage (see Section 11).

### Verify Deployment

After deployment, confirm the Function App is running and events are flowing:

```bash
# Check Function App logs (streaming)
az webapp log tail \
  --name <your-function-app-name> \
  --resource-group rg-weather-streaming
```

In the Microsoft Fabric Workspace, open the Eventstream and verify that events are appearing in the preview panel within 30–60 seconds of enabling the stream.

Run this KQL query in the Eventhouse to confirm data arrival:

```kql
['weather-table-bq']
| order by EventEnqueuedUtcTime desc
| take 5
```

---

## 11. CI/CD Pipeline

### `azure-pipelines.yml` — Current Behavior

The pipeline triggers on every commit to the `main` branch and mirrors the repository to the GitHub public repository `Jeancmn/weather-streaming-bq-proyect`.

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

### Pipeline Variables

| Variable | Scope | Description |
|---|---|---|
| `GITHUB_TOKEN` | Pipeline secret | Personal Access Token (PAT) with `repo` scope on the target GitHub repository |

To add the variable in Azure DevOps: **Pipelines → Edit → Variables → New Variable → check "Keep this value secret"**.

### Recommended Extension — Function App Deployment Stage

To automate Function App deployment, add the following stage after the mirror sync step:

```yaml
- task: UsePythonVersion@0
  inputs:
    versionSpec: '3.11'

- script: |
    pip install --target=".python_packages/lib/site-packages" -r requirements.txt
  workingDirectory: weather-streaming-function-app
  displayName: 'Install Python dependencies'

- task: ArchiveFiles@2
  inputs:
    rootFolderOrFile: 'weather-streaming-function-app'
    includeRootFolder: false
    archiveFile: '$(Build.ArtifactStagingDirectory)/functionapp.zip'

- task: AzureFunctionApp@2
  inputs:
    connectedServiceNameARM: '<your-service-connection>'
    appType: functionAppLinux
    appName: '<your-function-app-name>'
    package: '$(Build.ArtifactStagingDirectory)/functionapp.zip'
    runtimeStack: 'PYTHON|3.11'
```

---

## 12. Monitoring & Alerting

### Application Insights

The Function App is integrated with Azure Application Insights through `host.json`:

```json
{
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true,
        "excludedTypes": "Request"
      }
    }
  }
}
```

Key metrics to monitor:

| Metric | Alert Threshold | Meaning |
|---|---|---|
| Function execution count | < 2 per minute | Timer may have stopped |
| Function failure rate | > 5% | API or Event Hub errors |
| Event Hub incoming messages | < 1 per minute | Ingestion gap |
| Average execution duration | > 10,000 ms | Latency or timeout risk |

### Fabric Reflex Alerting

The **Weather Alerts.Reflex** rule runs every **60 seconds** and detects new weather advisory records in the KQL table. Upon detection, it sends an email with:

- **Subject:** `Weather Alert`
- **Headline:** `A weather alert has been received in Barranquilla`
- **Body fields:** `AlertValue` (full alert JSON), `LastTriggered` (timestamp)

This alerting mechanism is entirely managed within Microsoft Fabric and does not require any external notification service.

---

## 13. Power BI Report

The Fabric Workspace contains a two-page Power BI report connected to the KQL Semantic Model.

### Semantic Model Tables

| Table | Source | Description |
|---|---|---|
| `history_weather_data` | KQL Database | Historical time series — all ingested records |
| `Latest_weather_data` | KQL Database | Most recent observation per location |

### Report Pages

#### Page 1 — Live Conditions

Real-time snapshot of current meteorological conditions: temperature, feels-like temperature, wind speed and direction, humidity, UV index, and sky condition.

![Live Conditions](assets/Live%20Conditions.png)

#### Page 2 — Air Quality & Atmosphere

Atmospheric and air quality indicators: CO, NO₂, O₃, SO₂, PM2.5, PM10, pressure, precipitation, and cloud cover — including EPA and DEFRA indexes.

![Air Quality & Atmosphere](assets/Air%20Quality%20%26%20Atmosphere.png)

The Semantic Model uses **DirectLake** mode, which reads data directly from the Eventhouse Delta tables without import, providing near-real-time refresh latency.

---

## 14. Security Considerations

### Implemented Controls

| Control | Implementation |
|---|---|
| No hardcoded credentials | All secrets stored in Azure Key Vault; accessed via Managed Identity |
| Least privilege identity | Function App Managed Identity has only `Key Vault Secrets User` and `Event Hubs Data Sender` roles |
| No credential in source code | `local.settings.json` is in `.gitignore`; secrets never committed |
| Transport encryption | All connections use TLS 1.2+ (HTTPS to WeatherAPI, AMQP-over-TLS to Event Hub) |
| Secret rotation support | Rotating `weatherapikey` in Key Vault takes effect on the next function execution with no code change |

### Recommendations for Production Hardening

- Enable **Private Endpoints** on Event Hub and Key Vault to restrict access to the Azure virtual network.
- Use a **Dedicated** or **Premium** Azure Function App plan to avoid cold-start latency and enable VNet integration.
- Enable **Soft Delete** and **Purge Protection** on Key Vault.
- Set up **Diagnostic Settings** on the Event Hub namespace to forward metrics to Log Analytics.
- Rotate the `GITHUB_TOKEN` pipeline secret periodically and store it with a short expiry.
- Consider replacing the Event Hub connection string in the Databricks notebook scope with a Managed Identity approach for consistency.

---

## 15. Troubleshooting

### Function App not executing

| Symptom | Likely cause | Resolution |
|---|---|---|
| No executions in Application Insights | Function App stopped or timer misconfigured | Verify `*/30 * * * * *` in `function_app.py`; start the Function App |
| `DefaultAzureCredential` authentication failure | Managed Identity not enabled or missing RBAC role | Confirm identity is enabled; re-run Step 5 of the infrastructure setup |
| Key Vault access denied (403) | RBAC assignment not propagated | Wait up to 5 minutes for RBAC to propagate; verify role assignment in Azure Portal |
| Event Hub send failure | Wrong namespace or Event Hub name | Confirm values in `function_app.py` match the provisioned resources |

### No data in KQL table

| Symptom | Likely cause | Resolution |
|---|---|---|
| Eventstream shows events but table is empty | Table schema mismatch | Run the `.create-merge table` DDL from Section 5 Step 9 |
| Eventstream shows no events | Consumer group offset issue | Reset the consumer group offset in the Eventstream source settings |
| Events in Event Hub but not reaching Eventstream | Eventstream not activated | Click **Activate** in the Eventstream canvas |

### WeatherAPI errors

| HTTP Status | Meaning | Resolution |
|---|---|---|
| 401 | Invalid or missing API key | Check `weatherapikey` secret value in Key Vault |
| 403 | Key does not have access to AQI or Alerts endpoint | Upgrade WeatherAPI plan or disable AQI/Alerts in the payload |
| 429 | Rate limit exceeded | The free tier allows 1 million calls/month — 30s cadence = ~86,400 calls/day. Verify plan limits |

### Reflex alert not firing

- Confirm the rule `shouldRun` is set to `true` in **Weather Alerts.Reflex**.
- Verify the KQL source query returns rows when executed manually in the KQL Queryset.
- Check that the target email address is valid and not blocked by your organization's mail filter.

---

## 16. Repository Structure

```
weather-streaming-bq/
│
├── azure-pipelines.yml                         # CI/CD pipeline — triggers on main, mirrors to GitHub
├── README.md                                   # This document
│
├── weather-streaming-function-app/              # Azure Function App (production ingestion)
│   ├── function_app.py                         # Timer-triggered function — fetches weather & publishes to Event Hub
│   ├── host.json                               # Azure Functions runtime config (v2, Extension Bundle 4.x)
│   ├── local.settings.json                     # Local dev settings — NOT committed to production
│   ├── requirements.txt                        # Python dependencies
│   ├── .funcignore                             # Files excluded from Function App deployment package
│   └── .gitignore                             # Excludes local.settings.json and virtual environments
│
├── databricks-weather-streaming-notebooks/     # Development & testing notebooks (not production)
│   └── weather-streaming-notebook.py           # Iterative notebook: API test → Event Hub test → streaming
│
└── fabric-weather-streaming-ws/                # Microsoft Fabric Workspace items
    ├── Readme.md
    ├── for-alerts.KQLQueryset/                 # KQL query for alert detection logic
    │   └── RealTimeQueryset.json
    ├── Weather Alerts.Reflex/                  # Data Activator rule — email on new weather alerts
    │   └── ReflexEntities.json
    ├── weather-eventhouse-bq.Eventhouse/        # Eventhouse container
    │   └── .children/
    │       └── weather-eventhouse-bq.KQLDatabase/
    │           ├── DatabaseSchema.kql           # Table DDL — weather-table-bq
    │           ├── DatabaseProperties.json
    │           └── EmbeddedRealTimeQueryset.json
    ├── weather-eventstream-bq.Eventstream/      # Eventstream — routes Event Hub → Eventhouse
    │   ├── eventstream.json                    # Source, stream, and destination topology
    │   └── eventstreamProperties.json
    ├── Weather-Report-Barranquilla-Colombia.Report/    # Power BI Report definition
    │   ├── definition/                         # Pages and visual definitions (JSON)
    │   └── StaticResources/                    # Themes, images
    └── Weather-Report-Barranquilla-Colombia.SemanticModel/  # Power BI Semantic Model
        └── definition/
            ├── model.tmdl                      # Model-level TMDL definition
            ├── relationships.tmdl              # Table relationships
            └── tables/                         # history_weather_data, Latest_weather_data, DateTables
```

---

## Authors

| Name | Role | Contact |
|---|---|---|
| Jean Carlos Mangones | Data Engineer | — |

---

*Last updated: June 2026*
