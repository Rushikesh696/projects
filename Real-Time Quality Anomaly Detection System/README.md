# Real-Time Quality Anomaly Detection System
### Serum Institute of India Pvt. Ltd. — Quality Assurance Division
**Document Reference:** QA-IT-2024-0047  
**Effective Date:** 01 January 2024  
**Classification:** Confidential — IT Engagement Document

---

## 1. Background

Serum Institute of India Pvt. Ltd. (SIIPL) is the world's largest vaccine manufacturer by volume, producing over 1.5 billion doses annually across multiple product lines including Covovax, BCG, Rota Vaccine, EPO, and Cy-Tb. The Corporate Plant Quality Assurance division tracks quality performance across 10+ KPI categories on a monthly basis through the Quality Performance Metrics (QPM) reporting system.

Current QPM reporting is retrospective — metrics are reviewed monthly and escalations are raised only after anomalies have already impacted operations. As manufacturing volumes scale, the lag between an anomalous event occurring and its detection through monthly review poses an unacceptable risk to batch quality, regulatory compliance, and patient safety.

---

## 2. Problem Statement

### 2.1 Core Problem

The existing Quality Performance Metrics system at SIIPL Corporate Plant generates quality event data in real time (deviations, complaints, OOS events, ADEs, CAPAs) but lacks any automated mechanism to detect, flag, or escalate anomalous patterns as they emerge.

Critical quality failures — such as a sudden surge in deviations from the Blending or Filling line, an unexpected spike in Out-of-Specification (OOS) results, or an abnormal cluster of Adverse Drug Events — are currently identified only during monthly management review cycles. This 4–6 week detection lag results in:

- Delayed CAPA initiation on repeat deviations
- Reactive rather than preventive batch rejection decisions
- Increased regulatory risk during external audits (WHO, US FDA, EMA)
- Reduced Lot Acceptance Rate (LAR) and elevated Product Quality Complaint Rate (PQCR)

### 2.2 Specific Pain Points

| # | Pain Point | Business Impact |
|---|-----------|-----------------|
| 1 | Deviation spikes from Blending, Filling, Lyophilization, and Capping systems go undetected for weeks | Batches already released before root cause identified |
| 2 | Complaint rate threshold breaches not flagged in real time | PQCR reported as elevated only at month-end review |
| 3 | OOS clusters in batch release and LT stability not correlated with system or product | Root cause analysis delayed, regulatory non-compliance risk |
| 4 | CAPA closure overdue rate rising (83% required timeline extensions in Jan 2025) | Repeat deviations recurring due to ineffective corrective actions |
| 5 | No automated severity escalation — Critical and Major events mixed in same queue | Critical events not prioritized for immediate management attention |

---

## 3. Objective

Design and implement a **Real-Time Quality Anomaly Detection System** that:

1. Ingests quality event data (deviations, complaints, OOS, ADE, CAPA) as a continuous stream
2. Detects statistically significant anomalies — spikes, clusters, and threshold breaches — within minutes of occurrence
3. Classifies anomaly severity (Critical / Major / Minor) and identifies the affected system and product
4. Triggers automated alerts to the relevant QA personnel and management
5. Provides a live monitoring dashboard for QA officers to track ongoing quality events

---

## 4. Scope of Work

### In Scope
- Real-time ingestion of quality events from the QPM system
- Anomaly detection across the following event types:
  - Deviations (by system: Blending, Filling, Lyophilization, Capping, Manufacturing)
  - Product Complaints (by severity: Critical, Major, Minor)
  - Out-of-Specification (OOS) events (batch release and LT stability)
  - Adverse Drug Events (ADE)
  - CAPA closure overdue detection
- Automated alerting via email / dashboard notification
- Live monitoring dashboard (web-based)
- Historical baseline model trained on minimum 24 months of QPM event data

### Out of Scope
- Integration with SAP or ERP systems
- Modification of existing QPM report format or SOP
- Regulatory submission documentation
- Mobile application

---

## 5. Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-01 | System shall ingest quality events within 60 seconds of logging |
| FR-02 | System shall detect deviation rate spikes exceeding 2.5x the 30-day rolling average for any manufacturing system |
| FR-03 | System shall raise a Critical alert when complaint rate (PQCR) exceeds 0.15 per million doses in any rolling 7-day window |
| FR-04 | System shall flag OOS clusters of 3 or more events from the same product or system within a 72-hour window |
| FR-05 | System shall identify and flag duplicate event entries within the event stream |
| FR-06 | System shall classify each anomaly by type, severity, affected system, and product |
| FR-07 | System shall send automated email/SMS alert to QA Head within 15 minutes of a Critical anomaly detection |
| FR-08 | Dashboard shall display live event feed, anomaly flags, and 7-day trend charts |
| FR-09 | System shall maintain a minimum precision of 85% and recall of 80% on anomaly detection |
| FR-10 | All alerts and detections shall be logged with timestamp and audit trail for regulatory review |

---

## 6. Technical Requirements

| ID | Requirement |
|----|-------------|
| TR-01 | System shall use a streaming data pipeline capable of processing minimum 500 events/hour |
| TR-02 | ML model shall be retrained automatically on a monthly basis with new QPM data |
| TR-03 | System shall operate on-premise or private cloud to comply with SIIPL data confidentiality policy |
| TR-04 | All data at rest and in transit shall be encrypted (AES-256 / TLS 1.2+) |
| TR-05 | System shall maintain 99.5% uptime during manufacturing hours (06:00–22:00 IST) |
| TR-06 | Dashboard shall be accessible via standard web browser with role-based access control |

---

## 7. Data Available

The following quality event data is available for model training and system integration:

| Data Source | Type | Volume | Frequency |
|------------|------|--------|-----------|
| QPM Monthly Report | Aggregated KPIs | 10+ metrics | Monthly |
| Event-level QA logs | Deviations, Complaints, OOS, ADE, CAPA | ~17,000+ events/2 years | Real-time |
| Product list | Covovax, BCG, Rota, EPO, Cy-Tb | 8 products | Static |
| System list | Blending, Filling, Lyophilization, Capping, Manufacturing, QC Lab | 7 systems | Static |

**Training Dataset:** `serum_qa_realtime_raw.csv`
- 17,469 records | Jan 2023 – Jan 2025
- 17 features per event
- Labelled anomaly column (`is_anomaly`) for supervised training
- Includes realistic data quality issues: missing values, duplicates, label inconsistencies, reporting delays

---

## 8. Deliverables Expected from IT Vendor

| # | Deliverable | Description |
|---|------------|-------------|
| D-01 | Data Cleaning Pipeline | Handles missing values, duplicates, severity normalization |
| D-02 | Anomaly Detection Model | Isolation Forest or Autoencoder-based, trained on provided dataset |
| D-03 | Streaming Pipeline | Simulated real-time event stream (Kafka or equivalent) |
| D-04 | Alert Engine | Rule-based + ML-based threshold alerting |
| D-05 | Monitoring Dashboard | Streamlit/web dashboard with live event feed and anomaly flags |
| D-06 | Model Evaluation Report | Precision, Recall, F1, confusion matrix, SHAP explainability |
| D-07 | Deployment Documentation | Setup, configuration, and user guide |

---

## 9. Evaluation Criteria

| Metric | Minimum Threshold |
|--------|------------------|
| Anomaly Detection Precision | ≥ 85% |
| Anomaly Detection Recall | ≥ 80% |
| False Positive Rate | ≤ 10% |
| Alert Latency (Critical events) | ≤ 15 minutes |
| Dashboard Load Time | ≤ 3 seconds |

---

## 10. Glossary

| Term | Definition |
|------|-----------|
| LAR | Lot Acceptance Rate — accepted UDs / total UDs taken |
| PQCR | Product Quality Complaint Rate — complaints per million doses distributed |
| IOOSR | Invalidated Out-of-Specification Rate — invalid OOS / total OOS |
| OOS | Out of Specification — test result outside established acceptance criteria |
| CAPA | Corrective Action and Preventive Action |
| ADE | Adverse Drug Event |
| UD | Unit Dose |
| LT Stability | Long-Term Stability (product shelf life testing) |
| QPM | Quality Performance Metrics |
| SIIPL | Serum Institute of India Pvt. Ltd. |

---

*This document is the property of Serum Institute of India Pvt. Ltd. and is intended solely for the purposes of IT vendor engagement. Unauthorized disclosure is prohibited.*
