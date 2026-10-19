# Freight History Tracking with Microsoft Fabric

## Overview

This project implements a freight history tracking solution using Microsoft Fabric.

Traditional semantic models expose only the current state of freight rates. When a freight value changes, previous values are overwritten and historical analysis becomes impossible.

This solution creates historical snapshots and stores only meaningful changes using hash-based validation.

---

## Business Problem

Freight rates are periodically updated by logistics teams.

Without versioning, it is not possible to answer questions such as:

- When did a freight value change?
- What was the previous value?
- How did freight rates evolve over time?
- Which routes experienced the highest increases?

---

## Solution Architecture

```text
Semantic Model
        ↓
Dataflow Gen2
        ↓
Lakehouse
        ↓
Current Freight Table
        ↓
PySpark Notebook
        ↓
Hash Validation
        ↓
Historical Snapshot
        ↓
Power BI
```

---

## Technologies

- Microsoft Fabric
- Dataflow Gen2
- Lakehouse
- Data Pipelines
- PySpark
- Delta Tables
- Power BI

---

## Change Detection Strategy

A hash is calculated from the business columns of the freight table.

If the current hash matches the previous hash:

```text
No changes detected
```

If the hash differs:

```text
Historical snapshot is generated
```

This prevents duplicated snapshots and stores only meaningful changes.

---

## Components

### Current Freight Table

```text
frete
```

Stores the latest freight rates loaded from the semantic model.

### Historical Freight Table

```text
frete_snapshot
```

Stores historical versions of freight rates.

### Control Table

```text
controle_frete
```

Stores the last processed hash.

---

## Pipeline Flow

```text
Dataflow Gen2
      ↓
Lakehouse
      ↓
Notebook
      ↓
Hash Comparison
      ↓
Snapshot Creation
```

---

## Key Features

✅ Historical freight tracking

✅ Hash-based change detection

✅ Automated snapshot generation

✅ Lakehouse architecture

✅ Data Pipeline orchestration

✅ Power BI integration

