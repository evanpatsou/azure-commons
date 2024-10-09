# Comprehensive Database Schema Documentation

## Table of Contents

- [Comprehensive Database Schema Documentation](#comprehensive-database-schema-documentation)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Schema Overview](#schema-overview)
    - [Entities and Relationships](#entities-and-relationships)
    - [Schema Diagram](#schema-diagram)
  - [Detailed Database Schema](#detailed-database-schema)
    - [1. Categories](#1-categories)
    - [2. Topics](#2-topics)
    - [3. Subtopics](#3-subtopics)
    - [4. Indicators](#4-indicators)
    - [5. Companies](#5-companies)
    - [6. Runs](#6-runs)
    - [7. Indicator Values](#7-indicator-values)
    - [8. Topic Values](#8-topic-values)
    - [9. Subtopic Values](#9-subtopic-values)
  - [Relationships](#relationships)
  - [Data Flow and Usage](#data-flow-and-usage)
    - [Data Collection and Storage](#data-collection-and-storage)
    - [Data Processing](#data-processing)
    - [Aggregation and Logging](#aggregation-and-logging)
  - [Triggers and Constraints](#triggers-and-constraints)
    - [Trigger: Validate Indicators in Runs](#trigger-validate-indicators-in-runs)
    - [Trigger: Enforce Indicator Value Data Type](#trigger-enforce-indicator-value-data-type)
  - [Indexing Strategy](#indexing-strategy)
    - [Indexes on `indicator_values`](#indexes-on-indicator_values)
    - [Indexes on `runs`](#indexes-on-runs)
    - [Indexes on Aggregated Tables](#indexes-on-aggregated-tables)
  - [Sample Data Insertion](#sample-data-insertion)
    - [Insert Categories](#insert-categories)
    - [Insert Topics](#insert-topics)
    - [Insert Subtopics](#insert-subtopics)
    - [Insert Indicators](#insert-indicators)
    - [Insert Companies](#insert-companies)
    - [Insert Runs](#insert-runs)
    - [Insert Indicator Values](#insert-indicator-values)
    - [Insert Topic Values](#insert-topic-values)
    - [Insert Subtopic Values](#insert-subtopic-values)
  - [Sample Queries](#sample-queries)
    - [Retrieve Indicator Values for a Specific Company and Run](#retrieve-indicator-values-for-a-specific-company-and-run)
    - [Calculate Subtopic Scores Dynamically](#calculate-subtopic-scores-dynamically)
    - [Retrieve Historical Topic Values](#retrieve-historical-topic-values)
      - [Create Indexes](#create-indexes)
      - [Create Triggers and Functions](#create-triggers-and-functions)
    - [Trigger Functions](#trigger-functions)
      - [Trigger: Validate Indicators in Runs](#trigger-validate-indicators-in-runs-1)
      - [Trigger: Enforce Indicator Value Data Type](#trigger-enforce-indicator-value-data-type-1)

---

## Introduction

This documentation provides a comprehensive overview of the database schema designed to manage hierarchical data involving **Categories**, **Topics**, **Subtopics**, **Indicators**, **Companies**, and **Runs**. The schema is optimized to balance performance and flexibility, accommodating complex requirements while maintaining efficiency. It facilitates tracking and logging of indicator values per company and run, as well as aggregated values for topics and subtopics processed externally by scripts.

---

## Schema Overview

### Entities and Relationships

- **Categories**
  - Represents top-level groupings.
  - Each category can have multiple topics.

- **Topics**
  - Associated with categories.
  - Each topic can have multiple subtopics.

- **Subtopics**
  - Associated with topics.
  - Each subtopic can have multiple indicators.

- **Indicators**
  - Associated with subtopics.
  - Have dynamic attributes stored as JSONB.
  - Can have different data types of attributes depending on the related subtopic.

- **Companies**
  - Represents entities for which indicators are evaluated.

- **Runs**
  - Represents each execution.
  - Records indicator values, topic values, subtopic values, and involved companies.

- **Indicator Values**
  - Stores the values of indicators per company and run.

- **Topic Values**
  - Stores aggregated values of topics per company and run.

- **Subtopic Values**
  - Stores aggregated values of subtopics per company and run.

### Schema Diagram

![Schema Diagram](schema_diagram.png)

*Note: Replace `schema_diagram.png` with your actual schema diagram image.*

---

## Detailed Database Schema

### 1. Categories

**Description:**
Represents top-level groupings within the system. Each category serves as a container for related topics.

**Table Name:** `categories`

| Column | Data Type    | Constraints  | Description          |
|--------|--------------|--------------|----------------------|
| id     | SERIAL       | PRIMARY KEY  | Unique identifier    |
| name   | VARCHAR(255) | NOT NULL     | Name of the category |

**Example:**
```sql
INSERT INTO categories (name) VALUES ('Financial Metrics'), ('Operational Metrics');
```

---

### 2. Topics

**Description:**
Associated with categories, each topic represents a specific area within a category. A topic can have multiple subtopics.

**Table Name:** `topics`

| Column       | Data Type    | Constraints                 | Description           |
|--------------|--------------|-----------------------------|-----------------------|
| id           | SERIAL       | PRIMARY KEY                 | Unique identifier     |
| category_id  | INT          | NOT NULL, FOREIGN KEY       | References `categories(id)` |
| name         | VARCHAR(255) | NOT NULL                    | Name of the topic     |

**Example:**
```sql
INSERT INTO topics (category_id, name) VALUES
(1, 'Revenue'),
(1, 'Expenses'),
(2, 'Production'),
(2, 'Logistics');
```

---

### 3. Subtopics

**Description:**
Associated with topics, each subtopic delves deeper into a topic. A subtopic can have multiple indicators.

**Table Name:** `subtopics`

| Column      | Data Type    | Constraints                 | Description                         |
|-------------|--------------|-----------------------------|-------------------------------------|
| id          | SERIAL       | PRIMARY KEY                 | Unique identifier                   |
| topic_id    | INT          | NOT NULL, FOREIGN KEY       | References `topics(id)`             |
| name        | VARCHAR(255) | NOT NULL                    | Name of the subtopic                |
| is_active   | BOOLEAN      | DEFAULT TRUE                | Indicates if the subtopic is active |

**Example:**
```sql
INSERT INTO subtopics (topic_id, name, is_active) VALUES
(1, 'Quarterly Revenue', TRUE),
(1, 'Annual Revenue', TRUE),
(2, 'Operational Costs', TRUE),
(3, 'Units Produced', TRUE),
(4, 'Delivery Times', TRUE);
```

---

### 4. Indicators

**Description:**
Associated with subtopics, each indicator measures a specific metric. Indicators have dynamic attributes stored as JSONB, allowing for flexibility in defining various properties depending on the related subtopic.

**Table Name:** `indicators`

| Column       | Data Type    | Constraints                 | Description                                |
|--------------|--------------|-----------------------------|--------------------------------------------|
| id           | SERIAL       | PRIMARY KEY                 | Unique identifier                          |
| subtopic_id  | INT          | NOT NULL, FOREIGN KEY       | References `subtopics(id)`                 |
| name         | VARCHAR(255) | NOT NULL                    | Name of the indicator                      |
| attributes   | JSONB        | NOT NULL                    | Dynamic attributes in JSONB format         |
| data_type    | VARCHAR(50)  | NOT NULL                    | Expected data type of the indicator value (e.g., 'numeric', 'text', 'boolean') |

**Example:**
```sql
INSERT INTO indicators (subtopic_id, name, attributes, data_type) VALUES
(1, 'Q1 Revenue', '{"unit": "USD"}', 'numeric'),
(1, 'Q2 Revenue', '{"unit": "USD"}', 'numeric'),
(5, 'Average Delivery Time', '{"unit": "days"}', 'numeric');
```

---

### 5. Companies

**Description:**
Represents entities for which indicators are evaluated. Each company can have multiple indicator values recorded across different runs.

**Table Name:** `companies`

| Column | Data Type    | Constraints          | Description           |
|--------|--------------|----------------------|-----------------------|
| id     | SERIAL       | PRIMARY KEY          | Unique identifier     |
| name   | VARCHAR(255) | NOT NULL, UNIQUE     | Name of the company   |

**Example:**
```sql
INSERT INTO companies (name) VALUES ('Company A'), ('Company B');
```

---

### 6. Runs

**Description:**
Represents each execution or occurrence where indicator values, topic values, and subtopic values are recorded for involved companies.

**Table Name:** `runs`

| Column        | Data Type | Constraints | Description                                  |
|---------------|-----------|-------------|----------------------------------------------|
| id            | SERIAL    | PRIMARY KEY | Unique identifier                            |
| run_date      | DATE      | NOT NULL    | The date of the run                          |
| run_timestamp | TIMESTAMP | NOT NULL    | The exact timestamp of the run (default NOW) |
| indicators    | JSONB     | NOT NULL    | JSONB array of indicator IDs used in the run |

**Example:**
```sql
INSERT INTO runs (run_date, indicators) VALUES
('2023-10-15', '[1, 2, 3]'),
('2023-10-16', '[1, 3]');
```

---

### 7. Indicator Values

**Description:**
Stores the values of indicators for each company and run. The `value` field is a JSONB object to accommodate different data types, and the `status` field indicates whether the value is raw or processed.

**Table Name:** `indicator_values`

| Column       | Data Type  | Constraints                             | Description                                          |
|--------------|------------|-----------------------------------------|------------------------------------------------------|
| id           | SERIAL     | PRIMARY KEY                             | Unique identifier                                    |
| run_id       | INT        | NOT NULL, FOREIGN KEY                   | References `runs(id)`                                |
| indicator_id | INT        | NOT NULL, FOREIGN KEY                   | References `indicators(id)`                          |
| company_id   | INT        | NOT NULL, FOREIGN KEY                   | References `companies(id)`                           |
| value        | JSONB      | NOT NULL                                | Indicator value in JSONB format                      |
| status       | VARCHAR(50)| NOT NULL DEFAULT 'raw'                  | Status of the indicator value ('raw', 'processed')   |

**Example:**
```sql
INSERT INTO indicator_values (run_id, indicator_id, company_id, value, status) VALUES
(1, 1, 1, '{"value": 55000}', 'processed'),
(1, 2, 1, '{"value": 160000}', 'processed'),
(1, 3, 1, '{"value": true}', 'processed'),
(1, 1, 2, '{"value": 60000}', 'processed'),
(1, 2, 2, '{"value": 170000}', 'processed'),
(1, 3, 2, '{"value": false}', 'processed'),
(2, 1, 1, '{"value": 58000}', 'processed'),
(2, 3, 1, '{"value": true}', 'processed');
```

---

### 8. Topic Values

**Description:**
Stores aggregated values of topics for each company and run. Aggregations (e.g., averages, sums) are performed by external scripts and stored in this table.

**Table Name:** `topic_values`

| Column      | Data Type  | Constraints                             | Description                                |
|-------------|------------|-----------------------------------------|--------------------------------------------|
| id          | SERIAL     | PRIMARY KEY                             | Unique identifier                          |
| run_id      | INT        | NOT NULL, FOREIGN KEY                   | References `runs(id)`                      |
| topic_id    | INT        | NOT NULL, FOREIGN KEY                   | References `topics(id)`                    |
| company_id  | INT        | NOT NULL, FOREIGN KEY                   | References `companies(id)`                 |
| value       | NUMERIC    | NOT NULL                                | Aggregated value for the topic             |

**Example:**
```sql
INSERT INTO topic_values (run_id, topic_id, company_id, value) VALUES
(1, 1, 1, 105000),
(1, 2, 1, 160000),
(1, 1, 2, 120000),
(1, 2, 2, 170000),
(2, 1, 1, 58000);
```

---

### 9. Subtopic Values

**Description:**
Stores aggregated values of subtopics for each company and run. Similar to `topic_values`, aggregations are performed by external scripts and stored here.

**Table Name:** `subtopic_values`

| Column      | Data Type  | Constraints                             | Description                                 |
|-------------|------------|-----------------------------------------|---------------------------------------------|
| id          | SERIAL     | PRIMARY KEY                             | Unique identifier                           |
| run_id      | INT        | NOT NULL, FOREIGN KEY                   | References `runs(id)`                       |
| subtopic_id | INT        | NOT NULL, FOREIGN KEY                   | References `subtopics(id)`                  |
| company_id  | INT        | NOT NULL, FOREIGN KEY                   | References `companies(id)`                  |
| value       | NUMERIC    | NOT NULL                                | Aggregated value for the subtopic           |

**Example:**
```sql
INSERT INTO subtopic_values (run_id, subtopic_id, company_id, value) VALUES
(1, 1, 1, 55000),
(1, 2, 1, 160000),
(1, 3, 1, 45),
(1, 1, 2, 60000),
(1, 2, 2, 170000),
(1, 3, 2, 50),
(2, 1, 1, 58000),
(2, 3, 1, 47);
```

---

## Relationships

- **Categories to Topics**: One-to-Many
- **Topics to Subtopics**: One-to-Many
- **Subtopics to Indicators**: One-to-Many
- **Indicators to Indicator Values**: One-to-Many
- **Runs to Indicator Values**: One-to-Many
- **Companies to Indicator Values**: One-to-Many
- **Runs to Topic Values**: One-to-Many
- **Topics to Topic Values**: One-to-Many
- **Runs to Subtopic Values**: One-to-Many
- **Subtopics to Subtopic Values**: One-to-Many

---

## Data Flow and Usage

### Data Collection and Storage

1. **Run Creation**:
   - A new run is initiated and recorded in the `runs` table, specifying the date, timestamp, and the indicators used (stored as a JSONB array).
   
2. **Indicator Value Recording**:
   - For each company involved in the run, indicator values are collected and stored in the `indicator_values` table.
   - The `value` field stores the actual measurement in JSONB format, accommodating various data types.

### Data Processing

1. **Processing Indicator Values**:
   - External scripts process the raw indicator values (e.g., data cleaning, normalization).
   - After processing, the `status` field in `indicator_values` is updated to 'processed'.

2. **Aggregating Values**:
   - External scripts calculate aggregated values for subtopics and topics based on the processed indicator values.
   - These aggregated values are then stored in the `subtopic_values` and `topic_values` tables, respectively.

### Aggregation and Logging

- **Subtopic and Topic Aggregations**:
  - Aggregations such as averages, sums, or other statistical measures are performed by external scripts.
  - Results are stored in their respective tables to maintain a historical log and optimize query performance.

- **External Script Workflow**:
  1. **Fetch Processed Indicator Values**:
     - Retrieve all `indicator_values` with `status = 'processed'` for a specific run.
  
  2. **Compute Aggregations**:
     - Calculate necessary aggregations for each subtopic and topic.
  
  3. **Store Aggregated Values**:
     - Insert the computed values into `subtopic_values` and `topic_values`.

---

## Triggers and Constraints

### Trigger: Validate Indicators in Runs

**Purpose**:
Ensures that all indicator IDs listed in the `indicators` JSONB array within the `runs` table exist in the `indicators` table.

**Trigger Function**:
```sql
CREATE OR REPLACE FUNCTION validate_run_indicators()
RETURNS TRIGGER AS $$
DECLARE
    indicator_id INT;
    invalid_ids INT[];
BEGIN
    -- Extract all indicator IDs from the JSONB array
    WITH extracted_ids AS (
        SELECT (jsonb_array_elements_text(NEW.indicators))::INT AS indicator_id
    )
    SELECT ARRAY_AGG(e.indicator_id) INTO invalid_ids
    FROM extracted_ids e
    LEFT JOIN indicators i ON i.id = e.indicator_id
    WHERE i.id IS NULL;

    -- If any invalid IDs are found, raise an exception
    IF array_length(invalid_ids, 1) > 0 THEN
        RAISE EXCEPTION 'Invalid indicator IDs in run %: %', NEW.id, invalid_ids;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

**Trigger Creation**:
```sql
CREATE TRIGGER trg_validate_run_indicators
BEFORE INSERT OR UPDATE ON runs
FOR EACH ROW
EXECUTE FUNCTION validate_run_indicators();
```

**Explanation**:
- The trigger function extracts all indicator IDs from the `indicators` JSONB array.
- It checks whether each ID exists in the `indicators` table.
- If any invalid IDs are found, the trigger raises an exception to prevent the operation.

---

### Trigger: Enforce Indicator Value Data Type

**Purpose**:
Ensures that the `value` in `indicator_values` matches the expected data type defined in the `indicators` table.

**Trigger Function**:
```sql
CREATE OR REPLACE FUNCTION enforce_indicator_value_data_type()
RETURNS TRIGGER AS $$
DECLARE
    expected_data_type VARCHAR(50);
BEGIN
    -- Fetch expected data type for the indicator
    SELECT data_type INTO expected_data_type
    FROM indicators
    WHERE id = NEW.indicator_id;

    IF expected_data_type IS NULL THEN
        RAISE EXCEPTION 'Data type for indicator_id % is not defined.', NEW.indicator_id;
    END IF;

    -- Validate the value based on expected data type
    IF expected_data_type = 'numeric' THEN
        IF (NEW.value->>'value')::numeric IS NULL THEN
            RAISE EXCEPTION 'Numeric value is required for indicator_id %', NEW.indicator_id;
        END IF;
    ELSIF expected_data_type = 'boolean' THEN
        IF (NEW.value->>'value')::boolean IS NULL THEN
            RAISE EXCEPTION 'Boolean value is required for indicator_id %', NEW.indicator_id;
        END IF;
    ELSIF expected_data_type = 'text' THEN
        IF NEW.value->>'value' IS NULL THEN
            RAISE EXCEPTION 'Text value is required for indicator_id %', NEW.indicator_id;
        END IF;
    ELSE
        RAISE EXCEPTION 'Unknown data type "%" for indicator_id %', expected_data_type, NEW.indicator_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

**Trigger Creation**:
```sql
CREATE TRIGGER validate_indicator_values
BEFORE INSERT OR UPDATE ON indicator_values
FOR EACH ROW
EXECUTE FUNCTION enforce_indicator_value_data_type();
```

**Explanation**:
- The trigger function retrieves the expected data type for the indicator from the `indicators` table.
- It validates the `value` field in `indicator_values` against this expected data type.
- If the data type does not match, it raises an exception to prevent invalid data from being inserted or updated.

---

## Indexing Strategy

Effective indexing is crucial for optimizing query performance, especially when dealing with large datasets and JSONB fields.

### Indexes on `indicator_values`

- **GIN Index on `value` JSONB Column**:
  ```sql
  CREATE INDEX idx_indicator_values_value ON indicator_values USING gin (value);
  ```
  *Purpose*: Optimizes queries that search within the `value` JSONB column.

- **Composite Index on `run_id`, `indicator_id`, `company_id`**:
  ```sql
  CREATE INDEX idx_indicator_values_run_indicator_company ON indicator_values (run_id, indicator_id, company_id);
  ```
  *Purpose*: Enhances performance for queries filtering on these three columns simultaneously.

- **Partial Index on Processed Values**:
  ```sql
  CREATE INDEX idx_indicator_values_processed ON indicator_values ((value->>'value')) WHERE status = 'processed';
  ```
  *Purpose*: Speeds up queries that only deal with processed indicator values.

### Indexes on `runs`

- **GIN Index on `indicators` JSONB Column**:
  ```sql
  CREATE INDEX idx_runs_indicators ON runs USING gin (indicators);
  ```
  *Purpose*: Optimizes queries that search for specific indicators within the `indicators` JSONB array.

### Indexes on Aggregated Tables

- **Index on `subtopic_values`**:
  ```sql
  CREATE INDEX idx_subtopic_values_run_subtopic_company ON subtopic_values (run_id, subtopic_id, company_id);
  ```

- **Index on `topic_values`**:
  ```sql
  CREATE INDEX idx_topic_values_run_topic_company ON topic_values (run_id, topic_id, company_id);
  ```

**Reasoning**:
- **GIN Indexes**: Suitable for JSONB fields to facilitate efficient searching.
- **Composite Indexes**: Improve performance for multi-column queries.
- **Partial Indexes**: Enhance performance for specific subsets of data, such as only processed values.

---

## Sample Data Insertion

### Insert Categories

```sql
INSERT INTO categories (name) VALUES
('Financial Metrics'),
('Operational Metrics');
```

### Insert Topics

```sql
INSERT INTO topics (category_id, name) VALUES
(1, 'Revenue'),
(1, 'Expenses'),
(2, 'Production'),
(2, 'Logistics');
```

### Insert Subtopics

```sql
INSERT INTO subtopics (topic_id, name, is_active) VALUES
(1, 'Quarterly Revenue', TRUE),
(1, 'Annual Revenue', TRUE),
(2, 'Operational Costs', TRUE),
(3, 'Units Produced', TRUE),
(4, 'Delivery Times', TRUE);
```

### Insert Indicators

```sql
-- Indicators for Quarterly Revenue Subtopic
INSERT INTO indicators (subtopic_id, name, attributes, data_type) VALUES
(1, 'Q1 Revenue', '{"unit": "USD"}', 'numeric'),
(1, 'Q2 Revenue', '{"unit": "USD"}', 'numeric');

-- Indicators for Annual Revenue Subtopic
INSERT INTO indicators (subtopic_id, name, attributes, data_type) VALUES
(2, 'Annual Revenue', '{"unit": "USD"}', 'numeric');

-- Indicators for Operational Costs Subtopic
INSERT INTO indicators (subtopic_id, name, attributes, data_type) VALUES
(3, 'Total Operational Costs', '{"unit": "USD"}', 'numeric');

-- Indicators for Units Produced Subtopic
INSERT INTO indicators (subtopic_id, name, attributes, data_type) VALUES
(4, 'Units Produced', '{"unit": "units"}', 'numeric');

-- Indicators for Delivery Times Subtopic
INSERT INTO indicators (subtopic_id, name, attributes, data_type) VALUES
(5, 'Average Delivery Time', '{"unit": "days"}', 'numeric');
```

### Insert Companies

```sql
INSERT INTO companies (name) VALUES
('Company A'),
('Company B');
```

### Insert Runs

```sql
INSERT INTO runs (run_date, indicators) VALUES
('2023-10-15', '[1, 2, 3]'),
('2023-10-16', '[1, 3]');
```

### Insert Indicator Values

```sql
-- For Run ID 1 and Company A
INSERT INTO indicator_values (run_id, indicator_id, company_id, value, status) VALUES
(1, 1, 1, '{"value": 55000}', 'processed'),
(1, 2, 1, '{"value": 160000}', 'processed'),
(1, 3, 1, '{"value": 45000}', 'processed');

-- For Run ID 1 and Company B
INSERT INTO indicator_values (run_id, indicator_id, company_id, value, status) VALUES
(1, 1, 2, '{"value": 60000}', 'processed'),
(1, 2, 2, '{"value": 170000}', 'processed'),
(1, 3, 2, '{"value": 50000}', 'processed');

-- For Run ID 2 and Company A
INSERT INTO indicator_values (run_id, indicator_id, company_id, value, status) VALUES
(2, 1, 1, '{"value": 58000}', 'processed'),
(2, 3, 1, '{"value": 47000}', 'processed');
```

### Insert Topic Values

```sql
-- For Run ID 1 and Company A
INSERT INTO topic_values (run_id, topic_id, company_id, value) VALUES
(1, 1, 1, 105000),  -- Sum of Q1 and Q2 Revenue
(1, 2, 1, 45000);   -- Operational Costs

-- For Run ID 1 and Company B
INSERT INTO topic_values (run_id, topic_id, company_id, value) VALUES
(1, 1, 2, 120000),  -- Sum of Q1 and Q2 Revenue
(1, 2, 2, 50000);   -- Operational Costs

-- For Run ID 2 and Company A
INSERT INTO topic_values (run_id, topic_id, company_id, value) VALUES
(2, 1, 1, 58000),   -- Q1 Revenue only
(2, 2, 1, 47000);   -- Operational Costs
```

### Insert Subtopic Values

```sql
-- For Run ID 1 and Company A
INSERT INTO subtopic_values (run_id, subtopic_id, company_id, value) VALUES
(1, 1, 1, 55000),   -- Q1 Revenue
(1, 2, 1, 160000),  -- Q2 Revenue
(1, 3, 1, 45000);   -- Total Operational Costs

-- For Run ID 1 and Company B
INSERT INTO subtopic_values (run_id, subtopic_id, company_id, value) VALUES
(1, 1, 2, 60000),   -- Q1 Revenue
(1, 2, 2, 170000),  -- Q2 Revenue
(1, 3, 2, 50000);   -- Total Operational Costs

-- For Run ID 2 and Company A
INSERT INTO subtopic_values (run_id, subtopic_id, company_id, value) VALUES
(2, 1, 1, 58000),   -- Q1 Revenue
(2, 3, 1, 47000);   -- Total Operational Costs
```

---

## Sample Queries

### Retrieve Indicator Values for a Specific Company and Run

**Purpose**: Fetch all processed indicator values for a particular company during a specific run.

```sql
SELECT
    c.name AS company_name,
    r.run_date,
    i.name AS indicator_name,
    iv.value->>'value' AS value
FROM
    indicator_values iv
JOIN
    companies c ON c.id = iv.company_id
JOIN
    runs r ON r.id = iv.run_id
JOIN
    indicators i ON i.id = iv.indicator_id
WHERE
    iv.company_id = 1 AND  -- Replace with desired company ID
    iv.run_id = 1 AND      -- Replace with desired run ID
    iv.status = 'processed';
```

**Sample Result**:

| company_name | run_date   | indicator_name          | value  |
|--------------|------------|-------------------------|--------|
| Company A    | 2023-10-15 | Q1 Revenue              | 55000  |
| Company A    | 2023-10-15 | Q2 Revenue              | 160000 |
| Company A    | 2023-10-15 | Total Operational Costs | 45000  |

### Calculate Subtopic Scores Dynamically

**Purpose**: Calculate aggregated values for subtopics based on processed indicator values for a specific run and company.

```sql
SELECT
    s.id AS subtopic_id,
    s.name AS subtopic_name,
    c.id AS company_id,
    c.name AS company_name,
    AVG((iv.value->>'value')::numeric) AS subtopic_score
FROM
    indicator_values iv
JOIN
    indicators i ON i.id = iv.indicator_id
JOIN
    subtopics s ON s.id = i.subtopic_id
JOIN
    companies c ON c.id = iv.company_id
WHERE
    iv.run_id = 1 AND                  -- Replace with desired run ID
    iv.status = 'processed' AND
    i.data_type = 'numeric'
GROUP BY
    s.id, s.name, c.id, c.name;
```

**Sample Result**:

| subtopic_id | subtopic_name          | company_id | company_name | subtopic_score |
|-------------|------------------------|------------|--------------|----------------|
| 1           | Quarterly Revenue      | 1          | Company A    | 55000          |
| 2           | Annual Revenue         | 1          | Company A    | 160000         |
| 3           | Operational Costs      | 1          | Company A    | 45000          |
| 1           | Quarterly Revenue      | 2          | Company B    | 60000          |
| 2           | Annual Revenue         | 2          | Company B    | 170000         |
| 3           | Operational Costs      | 2          | Company B    | 50000          |
```

### Retrieve Indicators Used in a Run

**Purpose**: List all indicators that were used in a specific run.

```sql
SELECT
    r.id AS run_id,
    r.run_date,
    i.id AS indicator_id,
    i.name AS indicator_name
FROM
    runs r,
    jsonb_array_elements_text(r.indicators) AS ind_id
JOIN
    indicators i ON i.id = ind_id::INT
WHERE
    r.id = 1;  -- Replace with desired run ID
```

**Sample Result**:

| run_id | run_date   | indicator_id | indicator_name          |
|--------|------------|--------------|-------------------------|
| 1      | 2023-10-15 | 1            | Q1 Revenue              |
| 1      | 2023-10-15 | 2            | Q2 Revenue              |
| 1      | 2023-10-15 | 3            | Total Operational Costs |

### Retrieve Historical Topic Values

**Purpose**: Fetch historical aggregated topic values for a specific company.

```sql
SELECT
    tv.run_id,
    r.run_date,
    t.id AS topic_id,
    t.name AS topic_name,
    tv.company_id,
    c.name AS company_name,
    tv.value AS topic_value
FROM
    topic_values tv
JOIN
    runs r ON r.id = tv.run_id
JOIN
    topics t ON t.id = tv.topic_id
JOIN
    companies c ON c.id = tv.company_id
WHERE
    tv.company_id = 1  -- Replace with desired company ID
ORDER BY
    r.run_date DESC;
```

**Sample Result**:

| run_id | run_date   | topic_id | topic_name        | company_id | company_name | topic_value |
|--------|------------|----------|--------------------|------------|--------------|-------------|
| 1      | 2023-10-15 | 1        | Revenue            | 1          | Company A    | 105000      |
| 1      | 2023-10-15 | 2        | Expenses           | 1          | Company A    | 45000       |
| 2      | 2023-10-16 | 1        | Revenue            | 1          | Company A    | 58000       |
| 1      | 2023-10-15 | 1        | Revenue            | 2          | Company B    | 120000      |
| 1      | 2023-10-15 | 2        | Expenses           | 2          | Company B    | 50000       |
```

### Retrieve Historical Subtopic Values

**Purpose**: Fetch historical aggregated subtopic values for a specific company.

```sql
SELECT
    sv.run_id,
    r.run_date,
    st.id AS subtopic_id,
    st.name AS subtopic_name,
    sv.company_id,
    c.name AS company_name,
    sv.value AS subtopic_value
FROM
    subtopic_values sv
JOIN
    runs r ON r.id = sv.run_id
JOIN
    subtopics st ON st.id = sv.subtopic_id
JOIN
    companies c ON c.id = sv.company_id
WHERE
    sv.company_id = 1  -- Replace with desired company ID
ORDER BY
    r.run_date DESC;
```

**Sample Result**:

| run_id | run_date   | subtopic_id | subtopic_name          | company_id | company_name | subtopic_value |
|--------|------------|-------------|------------------------|------------|--------------|----------------|
| 1      | 2023-10-15 | 1           | Quarterly Revenue      | 1          | Company A    | 55000          |
| 1      | 2023-10-15 | 2           | Annual Revenue         | 1          | Company A    | 160000         |
| 1      | 2023-10-15 | 3           | Operational Costs      | 1          | Company A    | 45000          |
| 2      | 2023-10-16 | 1           | Quarterly Revenue      | 1          | Company A    | 58000          |
| 2      | 2023-10-16 | 3           | Operational Costs      | 1          | Company A    | 47000          |
| 1      | 2023-10-15 | 1           | Quarterly Revenue      | 2          | Company B    | 60000          |
| 1      | 2023-10-15 | 2           | Annual Revenue         | 2          | Company B    | 170000         |
| 1      | 2023-10-15 | 3           | Operational Costs      | 2          | Company B    | 50000          |
```

---

## Performance and Flexibility Balance

The database schema is meticulously designed to balance performance and flexibility:

### Flexibility Achieved Through:

- **JSONB Fields**:
  - **Dynamic Attributes**: Indicators can have varying attributes without altering the schema.
  - **Flexible Indicator Values**: Supports multiple data types within the same column, accommodating future changes.

- **Simplified Schema**:
  - **Reduced Tables**: By eliminating unnecessary tables like `run_indicators`, the schema remains lean and easier to manage.
  - **Aggregated Tables Optional**: Aggregations are handled externally, avoiding database bloat while still maintaining historical logs.

### Performance Optimized By:

- **Strategic Indexing**:
  - **GIN Indexes**: Efficiently handle JSONB queries, crucial for searching within JSONB fields.
  - **Composite and Partial Indexes**: Enhance performance for common and specific query patterns.

- **Triggers and Constraints**:
  - **Efficient Data Validation**: Triggers ensure data integrity without introducing significant overhead.
  - **Minimized Complexity**: Keeping triggers simple maintains performance while enforcing essential rules.

- **External Aggregation**:
  - **Offloading Processing**: Aggregations handled by scripts reduce the computational load on the database.
  - **Optimized Read Operations**: Precomputed aggregated values can be quickly accessed when needed.

### Simplification Efforts:

- **Consistent Naming Conventions**: Enhances readability and maintainability.
- **Comprehensive Documentation**: Facilitates understanding and future development.
- **Modular Design**: Allows for easy updates and scalability as requirements evolve.

---

## Conclusion

The designed database schema effectively manages hierarchical data involving categories, topics, subtopics, indicators, companies, and runs. By leveraging JSONB fields for dynamic attributes and indicator values, the schema maintains flexibility without compromising on performance. Aggregated values for topics and subtopics are handled externally by scripts, ensuring that the database remains streamlined and efficient.

**Key Benefits**:

- **Flexibility**: Easily accommodates varying data types and dynamic attributes.
- **Performance**: Optimized through strategic indexing and efficient data validation.
- **Scalability**: Designed to handle growth in data volume and complexity.
- **Maintainability**: Simplified structure and thorough documentation facilitate ongoing management and development.

This balanced approach ensures that the system remains robust, adaptable, and efficient, meeting both current and future data management needs.

---

## Appendix

### SQL DDL Statements

#### Create Tables

```sql
-- Drop existing tables for a clean setup (if necessary)
DROP TABLE IF EXISTS subtopic_values CASCADE;
DROP TABLE IF EXISTS topic_values CASCADE;
DROP TABLE IF EXISTS indicator_values CASCADE;
DROP TABLE IF EXISTS runs CASCADE;
DROP TABLE IF EXISTS companies CASCADE;
DROP TABLE IF EXISTS indicators CASCADE;
DROP TABLE IF EXISTS subtopics CASCADE;
DROP TABLE IF EXISTS topics CASCADE;
DROP TABLE IF EXISTS categories CASCADE;

-- Create Categories Table
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Create Topics Table
CREATE TABLE topics (
    id SERIAL PRIMARY KEY,
    category_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
);

-- Create Subtopics Table
CREATE TABLE subtopics (
    id SERIAL PRIMARY KEY,
    topic_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
);

-- Create Indicators Table
CREATE TABLE indicators (
    id SERIAL PRIMARY KEY,
    subtopic_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    attributes JSONB NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    FOREIGN KEY (subtopic_id) REFERENCES subtopics(id) ON DELETE CASCADE
);

-- Create Companies Table
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- Create Runs Table
CREATE TABLE runs (
    id SERIAL PRIMARY KEY,
    run_date DATE NOT NULL,
    run_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    indicators JSONB NOT NULL
);

-- Create Indicator Values Table
CREATE TABLE indicator_values (
    id SERIAL PRIMARY KEY,
    run_id INT NOT NULL,
    indicator_id INT NOT NULL,
    company_id INT NOT NULL,
    value JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'raw',
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (indicator_id) REFERENCES indicators(id) ON DELETE CASCADE,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
    UNIQUE (run_id, indicator_id, company_id)
);

-- Create Topic Values Table
CREATE TABLE topic_values (
    id SERIAL PRIMARY KEY,
    run_id INT NOT NULL,
    topic_id INT NOT NULL,
    company_id INT NOT NULL,
    value NUMERIC NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
    UNIQUE (run_id, topic_id, company_id)
);

-- Create Subtopic Values Table
CREATE TABLE subtopic_values (
    id SERIAL PRIMARY KEY,
    run_id INT NOT NULL,
    subtopic_id INT NOT NULL,
    company_id INT NOT NULL,
    value NUMERIC NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (subtopic_id) REFERENCES subtopics(id) ON DELETE CASCADE,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
    UNIQUE (run_id, subtopic_id, company_id)
);
```

#### Create Indexes

```sql
-- Index on runs.indicators JSONB Column
CREATE INDEX idx_runs_indicators ON runs USING gin (indicators);

-- Indexes on indicator_values
CREATE INDEX idx_indicator_values_value ON indicator_values USING gin (value);
CREATE INDEX idx_indicator_values_run_indicator_company ON indicator_values (run_id, indicator_id, company_id);
CREATE INDEX idx_indicator_values_processed ON indicator_values ((value->>'value')) WHERE status = 'processed';

-- Indexes on topic_values
CREATE INDEX idx_topic_values_run_topic_company ON topic_values (run_id, topic_id, company_id);

-- Indexes on subtopic_values
CREATE INDEX idx_subtopic_values_run_subtopic_company ON subtopic_values (run_id, subtopic_id, company_id);
```

#### Create Triggers and Functions

```sql
-- Create Trigger Function to Validate Indicators in Runs
CREATE OR REPLACE FUNCTION validate_run_indicators()
RETURNS TRIGGER AS $$
DECLARE
    indicator_id INT;
    invalid_ids INT[];
BEGIN
    -- Extract all indicator IDs from the JSONB array
    WITH extracted_ids AS (
        SELECT (jsonb_array_elements_text(NEW.indicators))::INT AS indicator_id
    )
    SELECT ARRAY_AGG(e.indicator_id) INTO invalid_ids
    FROM extracted_ids e
    LEFT JOIN indicators i ON i.id = e.indicator_id
    WHERE i.id IS NULL;

    -- If any invalid IDs are found, raise an exception
    IF array_length(invalid_ids, 1) > 0 THEN
        RAISE EXCEPTION 'Invalid indicator IDs in run %: %', NEW.id, invalid_ids;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create Trigger on Runs Table
CREATE TRIGGER trg_validate_run_indicators
BEFORE INSERT OR UPDATE ON runs
FOR EACH ROW
EXECUTE FUNCTION validate_run_indicators();

-- Create Trigger Function to Enforce Indicator Value Data Type
CREATE OR REPLACE FUNCTION enforce_indicator_value_data_type()
RETURNS TRIGGER AS $$
DECLARE
    expected_data_type VARCHAR(50);
BEGIN
    -- Fetch expected data type for the indicator
    SELECT data_type INTO expected_data_type
    FROM indicators
    WHERE id = NEW.indicator_id;

    IF expected_data_type IS NULL THEN
        RAISE EXCEPTION 'Data type for indicator_id % is not defined.', NEW.indicator_id;
    END IF;

    -- Validate the value based on expected data type
    IF expected_data_type = 'numeric' THEN
        IF (NEW.value->>'value')::numeric IS NULL THEN
            RAISE EXCEPTION 'Numeric value is required for indicator_id %', NEW.indicator_id;
        END IF;
    ELSIF expected_data_type = 'boolean' THEN
        IF (NEW.value->>'value')::boolean IS NULL THEN
            RAISE EXCEPTION 'Boolean value is required for indicator_id %', NEW.indicator_id;
        END IF;
    ELSIF expected_data_type = 'text' THEN
        IF NEW.value->>'value' IS NULL THEN
            RAISE EXCEPTION 'Text value is required for indicator_id %', NEW.indicator_id;
        END IF;
    ELSE
        RAISE EXCEPTION 'Unknown data type "%" for indicator_id %', expected_data_type, NEW.indicator_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create Trigger on Indicator Values Table
CREATE TRIGGER validate_indicator_values
BEFORE INSERT OR UPDATE ON indicator_values
FOR EACH ROW
EXECUTE FUNCTION enforce_indicator_value_data_type();
```

---

### Trigger Functions

#### Trigger: Validate Indicators in Runs

**Purpose**: Ensures that all indicator IDs listed in the `indicators` JSONB array within the `runs` table exist in the `indicators` table.

**Function**:
```sql
CREATE OR REPLACE FUNCTION validate_run_indicators()
RETURNS TRIGGER AS $$
DECLARE
    indicator_id INT;
    invalid_ids INT[];
BEGIN
    -- Extract all indicator IDs from the JSONB array
    WITH extracted_ids AS (
        SELECT (jsonb_array_elements_text(NEW.indicators))::INT AS indicator_id
    )
    SELECT ARRAY_AGG(e.indicator_id) INTO invalid_ids
    FROM extracted_ids e
    LEFT JOIN indicators i ON i.id = e.indicator_id
    WHERE i.id IS NULL;

    -- If any invalid IDs are found, raise an exception
    IF array_length(invalid_ids, 1) > 0 THEN
        RAISE EXCEPTION 'Invalid indicator IDs in run %: %', NEW.id, invalid_ids;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

---

#### Trigger: Enforce Indicator Value Data Type

**Purpose**: Ensures that the `value` in `indicator_values` matches the expected data type defined in the `indicators` table.

**Function**:
```sql
CREATE OR REPLACE FUNCTION enforce_indicator_value_data_type()
RETURNS TRIGGER AS $$
DECLARE
    expected_data_type VARCHAR(50);
BEGIN
    -- Fetch expected data type for the indicator
   