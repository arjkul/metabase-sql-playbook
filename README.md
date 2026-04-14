# metabase-sql-playbook

> A curated library of documented SQL patterns for B2B warehouse operations analytics — built for Metabase on Snowflake.

---

## Overview

This playbook collects the SQL patterns behind ShipMonk's B2B wholesale operations BI system. Each query is documented with its business purpose, key design decisions, and caveats — not just the SQL itself.

The goal: any data analyst or PM joining the team can understand not just *what* these queries do, but *why* they're structured the way they are.

---

## Query Index

| # | Query | Business Question | System |
|---|---|---|---|
| 01 | [OTRS Weekly by Warehouse](#01-otrs-weekly-by-warehouse) | Are orders releasing on time? | Snowflake |
| 02 | [OTS Weekly by Warehouse](#02-ots-weekly-by-warehouse) | Are orders shipping on time and in full? | Snowflake |
| 03 | [Aging Orders by Account and Warehouse](#03-aging-orders-by-account-and-warehouse) | Which orders are at risk of missing SLA? | Snowflake |
| 04 | [Storage Fee Billing Gap Detection](#04-storage-fee-billing-gap-detection) | Which wholesale orders were eligible for storage fees but never billed? | Snowflake |
| 05 | [Pick Audit Weekly by Warehouse](#05-pick-audit-weekly-by-warehouse) | Is pick accuracy holding across sites? | Snowflake |
| 06 | [ASN Send Event History](#06-asn-send-event-history) | Were ASN notifications sent for EDI orders? | PostgreSQL (ACCI) |

---

## 01. OTRS Weekly by Warehouse

**Business question**: Of all wholesale orders due for release last week, what percentage were released on time?

**Design notes**:
- Uses `DATE_TRUNC('week', ...)` to align to Monday boundaries for W-1/W-2 comparison
- Counts nulls in `release_date` as misses (unreleased = not on time)
- Filters to wholesale order type only

```sql
SELECT
    w.warehouse_name,
    DATE_TRUNC('week', o.sla_date)        AS week_start,
    COUNT(*)                               AS total_orders,
    SUM(
        CASE WHEN o.release_date <= o.sla_date THEN 1 ELSE 0 END
    )                                      AS on_time_count,
    ROUND(
        SUM(CASE WHEN o.release_date <= o.sla_date THEN 1 ELSE 0 END)
        * 100.0 / NULLIF(COUNT(*), 0), 1
    )                                      AS otrs_pct
FROM wholesale_orders o
JOIN warehouses w ON o.warehouse_id = w.warehouse_id
WHERE
    o.order_type = 'wholesale'
    AND o.sla_date >= DATEADD('week', -2, DATE_TRUNC('week', CURRENT_DATE))
    AND o.sla_date <  DATE_TRUNC('week', CURRENT_DATE)
GROUP BY 1, 2
ORDER BY 2 DESC, 1
```

---

## 02. OTS Weekly by Warehouse

**Business question**: Of all released wholesale orders, what percentage shipped on time and in full?

**Design notes**:
- Two miss conditions: late ship date OR short quantity
- Uses LEFT JOIN to shipments to catch orders that were released but never shipped
- `NULLIF` guards against division by zero on sites with no activity in a given week

```sql
SELECT
    w.warehouse_name,
    DATE_TRUNC('week', o.release_date)     AS week_start,
    COUNT(o.order_number)                  AS released_orders,
    SUM(
        CASE
            WHEN s.ship_timestamp <= o.ship_sla_date
             AND s.qty_shipped >= o.qty_ordered THEN 1
            ELSE 0
        END
    )                                      AS on_time_in_full,
    ROUND(
        SUM(CASE
            WHEN s.ship_timestamp <= o.ship_sla_date
             AND s.qty_shipped >= o.qty_ordered THEN 1
            ELSE 0
        END) * 100.0 / NULLIF(COUNT(o.order_number), 0), 1
    )                                      AS ots_pct
FROM wholesale_orders o
LEFT JOIN shipments s ON o.order_number = s.order_number
JOIN warehouses w ON o.warehouse_id = w.warehouse_id
WHERE
    o.release_date >= DATEADD('week', -2, DATE_TRUNC('week', CURRENT_DATE))
    AND o.release_date <  DATE_TRUNC('week', CURRENT_DATE)
GROUP BY 1, 2
ORDER BY 2 DESC, 1
```

---

## 03. Aging Orders by Account and Warehouse

**Business question**: Which open wholesale orders have been sitting the longest, and where are they?

**Design notes**:
- Filters to open (not shipped, not cancelled) orders only
- `days_aged` is calculated from order creation — not from when the order was released
- Used in the weekly ops meeting Building Headlines section

```sql
SELECT
    w.warehouse_name,
    m.merchant_name                        AS account_name,
    o.order_number,
    o.po_number,
    o.order_date,
    DATEDIFF('day', o.order_date, CURRENT_DATE)  AS days_aged,
    o.status
FROM wholesale_orders o
JOIN warehouses w ON o.warehouse_id = w.warehouse_id
JOIN merchants m ON o.merchant_id = m.merchant_id
WHERE
    o.status NOT IN ('shipped', 'cancelled', 'voided')
    AND o.order_type = 'wholesale'
ORDER BY days_aged DESC
```

---

## 04. Storage Fee Billing Gap Detection

**Business question**: Which wholesale orders were in storage long enough to be eligible for storage fees, but were never billed?

**Design notes**:
- `billed_orders` CTE uses a 60-day lookback to capture recent billing activity
- LEFT JOIN with `WHERE b.description IS NULL` isolates unbilled orders
- Joins to account tables to filter active merchants only (`ACTIVE = 1`)
- This query was the basis for a revenue integrity investigation surfacing previously undetected billing gaps

```sql
WITH eligible_orders AS (
    SELECT
        o.order_number,
        o.merchant_id,
        o.warehouse_id,
        o.order_date,
        o.ship_date,
        DATEDIFF('day', o.order_date, COALESCE(o.ship_date, CURRENT_DATE)) AS days_in_storage
    FROM wholesale_orders o
    WHERE
        o.order_type = 'wholesale'
        AND o.status IN ('open', 'shipped')
        AND DATEDIFF('day', o.order_date, COALESCE(o.ship_date, CURRENT_DATE)) >= 30
),

billed_orders AS (
    SELECT DISTINCT order_number, description
    FROM billing_line_items
    WHERE
        description ILIKE '%storage%'
        AND billing_date >= DATEADD('day', -60, CURRENT_DATE)
)

SELECT
    e.order_number,
    e.merchant_id,
    m.merchant_name,
    w.warehouse_name,
    e.order_date,
    e.ship_date,
    e.days_in_storage
FROM eligible_orders e
LEFT JOIN billed_orders b ON e.order_number = b.order_number
JOIN merchants m ON e.merchant_id = m.merchant_id
JOIN warehouses w ON e.warehouse_id = w.warehouse_id
WHERE
    b.description IS NULL       -- Never billed for storage
    AND m.active = 1            -- Active merchants only
ORDER BY e.days_in_storage DESC
```

---

## 05. Pick Audit Weekly by Warehouse

**Business question**: What percentage of audited picks were error-free, by warehouse, week over week?

```sql
SELECT
    w.warehouse_name,
    DATE_TRUNC('week', a.audit_date)       AS week_start,
    COUNT(*)                               AS audits_conducted,
    SUM(CASE WHEN a.error_count = 0 THEN 1 ELSE 0 END) AS clean_picks,
    ROUND(
        SUM(CASE WHEN a.error_count = 0 THEN 1 ELSE 0 END)
        * 100.0 / NULLIF(COUNT(*), 0), 1
    )                                      AS pick_accuracy_pct
FROM pick_audits a
JOIN warehouses w ON a.warehouse_id = w.warehouse_id
WHERE
    a.audit_date >= DATEADD('week', -2, DATE_TRUNC('week', CURRENT_DATE))
GROUP BY 1, 2
ORDER BY 2 DESC, 1
```

---

## 06. ASN Send Event History

**Business question**: For a set of EDI purchase orders, were ASN (856) notifications successfully sent and acknowledged?

**System**: PostgreSQL — ACCI database (db 31 prod / db 32 staging)

**Design notes**:
- Uses `order_references->>'poNumber'` for JSONB field extraction (not a regular column)
- Targets transaction_type = '856' specifically (ASN)
- Error messages often contain "Acknowledgment already exists" for duplicate sends

```sql
SELECT
    id,
    order_references->>'poNumber'          AS po_number,
    transaction_type,
    status,
    created_at,
    updated_at,
    error_message
FROM edi_notifications
WHERE
    order_references->>'poNumber' = ANY(ARRAY['PO-001', 'PO-002', 'PO-003'])
    AND transaction_type = '856'
ORDER BY created_at ASC
```

---

## 07. Metabase Parameter Debugging Patterns

**Business question**: Why is my Metabase card throwing a SQL compilation error, and how do I fix field filters and dynamic date parameters?

This section documents two recurring Metabase-on-Snowflake debugging patterns.

---

### Pattern A: Field Filter Causing Compilation Error

**Symptom**: `SQL compilation error: Object 'TABLE.COLUMN' does not exist` when using a field filter widget (`{{field_name}}`).

**Root cause**: Field filters map to a physical table column. If the column is derived from a CTE (not a real table), Metabase fails to resolve it.

**Fix**: Replace with optional plain parameter syntax:

```sql
-- ❌ Field filter — breaks with CTE-derived columns
WHERE sn.SUBMIT_DATE = {{sn.SUBMIT_DATE}}

-- ✅ Optional plain parameter
WHERE 1=1
  [[AND sn.SUBMIT_DATE >= {{start_date}}]]
  [[AND sn.SUBMIT_DATE <  {{end_date}}]]
```

---

### Pattern B: Dynamic Lookback Window with DATEADD

**Metabase parameter type**: `Number`

```sql
-- ❌ MySQL syntax — does not work in Snowflake
WHERE submit_date >= DATEDIFF('day', {{Days_Old}}, CURRENT_TIMESTAMP())

-- ✅ Snowflake syntax
WHERE sn.SUBMIT_DATE >= DATEADD('day', -{{Days_Old}}, CURRENT_TIMESTAMP())
```

**Full example — order processing events with dynamic lookback:**

```sql
WITH processing_events AS (
    SELECT
        order_id,
        submit_date,
        previous_submit_date,
        warehouse_id,
        queue_name,
        changed_by_user_id
    FROM SHIPMONK_BINLOG.BDM.ORDER_PROCESSING_EVENTS
    WHERE
        event_type = 'processing'
        AND submit_date >= DATEADD('day', -{{Days_Old}}, CURRENT_TIMESTAMP())
),
user_roles AS (
    SELECT user_id, role_name
    FROM ANALYTICS.BDM.USER_ROLES
    WHERE active = 1
)
SELECT
    pe.order_id,
    pe.submit_date,
    pe.queue_name,
    ur.role_name AS changed_by_role,
    w.warehouse_name
FROM processing_events pe
LEFT JOIN user_roles ur ON pe.changed_by_user_id = ur.user_id
JOIN SHIPMONK_BINLOG.BDM.WAREHOUSES w ON pe.warehouse_id = w.warehouse_id
ORDER BY pe.submit_date DESC
```

**Metabase setup**: Parameter name `Days_Old`, type `Number`, default `14`.

---

## Usage Notes

- All Snowflake queries use the `SHIPMONK_BINLOG.BDM` schema path for cross-database references
- Table and column names are sanitized from production — adapt field names to your actual schema
- Queries are designed for Metabase but will run in any standard SQL interface connected to Snowflake

---

## Related Repos

- [`warehouse-ops-intelligence`](https://github.com/arjkul/warehouse-ops-intelligence) — How these queries are used in the ops BI system
- [`b2b-chargeback-rca-framework`](https://github.com/arjkul/b2b-chargeback-rca-framework) — Where Query 06 (ASN history) fits into RCA
- [`ai-ops-tooling`](https://github.com/arjkul/ai-ops-tooling) — Automation built on top of these SQL patterns
