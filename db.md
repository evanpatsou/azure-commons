## Overview

The schema is designed to track **indicators** (like temperature, humidity) and their associated **attributes** (e.g., max value, min value), which are recorded during **runs**. The data is provided by **vendors** and managed by **issuers**. The schema supports **many-to-many relationships** between **indicators** and **attributes** and partitions the `Indicator_Values` table by specific days to improve query performance.

### Schema Entities:

1. **Vendors**: Represents companies providing indicator data (e.g., WeatherCorp).
2. **Issuers**: Represents organizations responsible for tracking the runs (e.g., GlobalWeather).
3. **Runs**: Represents a specific occurrence when indicators are tracked (e.g., a run on 2024-09-11).
4. **Indicators**: Represents metrics being tracked (e.g., Temperature, Humidity).
5. **Attributes**: Represents specific characteristics of an indicator (e.g., Max Temperature, Min Temperature).
6. **Run_Indicators**: Establishes the relationship between runs and indicators.
7. **Indicator_Values**: Stores the actual values of indicators provided by vendors, partitioned by specific days.
8. **Attribute_Values**: Stores the values for specific attributes associated with runs and issuers.
9. **Alternative_Indicators**: Defines alternative indicators for a given indicator (e.g., Celsius vs. Fahrenheit).

---

## SQL Initialization Script

```sql
-- Drop existing tables to avoid conflicts
DROP TABLE IF EXISTS Alternative_Indicators CASCADE;
DROP TABLE IF EXISTS Indicator_Attributes CASCADE;
DROP TABLE IF EXISTS Attribute_Values CASCADE;
DROP TABLE IF EXISTS Run_Indicators CASCADE;
DROP TABLE IF EXISTS Indicator_Values CASCADE;
DROP TABLE IF EXISTS Attributes CASCADE;
DROP TABLE IF EXISTS Indicators CASCADE;
DROP TABLE IF EXISTS Runs CASCADE;
DROP TABLE IF EXISTS Vendors CASCADE;
DROP TABLE IF EXISTS Issuers CASCADE;

-- Vendors Table
CREATE TABLE Vendors (
    vendor_id SERIAL PRIMARY KEY,
    vendor_name VARCHAR(255) NOT NULL
);

-- Issuers Table
CREATE TABLE Issuers (
    issuer_id SERIAL PRIMARY KEY,
    issuer_name VARCHAR(255) NOT NULL
);

-- Runs Table
CREATE TABLE Runs (
    run_id SERIAL PRIMARY KEY,
    run_date DATE NOT NULL
);

-- Indicators Table
CREATE TABLE Indicators (
    indicator_id SERIAL PRIMARY KEY,
    indicator_name VARCHAR(255) NOT NULL,
    UNIQUE (indicator_name)
);

-- Run_Indicators Table
CREATE TABLE Run_Indicators (
    run_id INT NOT NULL REFERENCES Runs(run_id) ON DELETE CASCADE,
    indicator_id INT NOT NULL REFERENCES Indicators(indicator_id) ON DELETE CASCADE,
    PRIMARY KEY (run_id, indicator_id)
);

-- Indicator_Values Table (Partitioned by specific days)
CREATE TABLE Indicator_Values (
    indicator_value_id SERIAL PRIMARY KEY,
    indicator_id INT NOT NULL REFERENCES Indicators(indicator_id) ON DELETE CASCADE,
    run_id INT NOT NULL REFERENCES Runs(run_id) ON DELETE CASCADE,
    issuer_id INT NOT NULL REFERENCES Issuers(issuer_id) ON DELETE CASCADE,
    vendor_id INT NOT NULL REFERENCES Vendors(vendor_id) ON DELETE CASCADE,
    value_date DATE NOT NULL,
    value DECIMAL(10, 2) NOT NULL,
    UNIQUE (indicator_id, run_id, vendor_id, value_date)
) PARTITION BY RANGE (value_date);

-- Partition the Indicator_Values table for specific days
CREATE TABLE Indicator_Values_20240911 PARTITION OF Indicator_Values
FOR VALUES FROM ('2024-09-11') TO ('2024-09-12');

CREATE TABLE Indicator_Values_20240912 PARTITION OF Indicator_Values
FOR VALUES FROM ('2024-09-12') TO ('2024-09-13');

CREATE TABLE Indicator_Values_20240913 PARTITION OF Indicator_Values
FOR VALUES FROM ('2024-09-13') TO ('2024-09-14');

-- Attributes Table
CREATE TABLE Attributes (
    attribute_id SERIAL PRIMARY KEY,
    attribute_name VARCHAR(255) NOT NULL,
    description TEXT
);

-- Indicator_Attributes Table (Many-to-Many relationship between Indicators and Attributes)
CREATE TABLE Indicator_Attributes (
    indicator_id INT NOT NULL REFERENCES Indicators(indicator_id) ON DELETE CASCADE,
    attribute_id INT NOT NULL REFERENCES Attributes(attribute_id) ON DELETE CASCADE,
    PRIMARY KEY (indicator_id, attribute_id)
);

-- Attribute_Values Table
CREATE TABLE Attribute_Values (
    attribute_value_id SERIAL PRIMARY KEY,
    attribute_id INT NOT NULL REFERENCES Attributes(attribute_id) ON DELETE CASCADE,
    run_id INT NOT NULL REFERENCES Runs(run_id) ON DELETE CASCADE,
    issuer_id INT NOT NULL REFERENCES Issuers(issuer_id) ON DELETE CASCADE,
    value DECIMAL(10, 2) NOT NULL,
    UNIQUE (attribute_id, run_id, issuer_id)
);

-- Alternative_Indicators Table
CREATE TABLE Alternative_Indicators (
    indicator_id INT NOT NULL REFERENCES Indicators(indicator_id) ON DELETE CASCADE,
    alternative_indicator_id INT NOT NULL REFERENCES Indicators(indicator_id) ON DELETE CASCADE,
    PRIMARY KEY (indicator_id, alternative_indicator_id)
);
```

---

## PlantUML Diagram

The following **PlantUML** diagram visually represents the schema described above:

```plantuml
@startuml
!define TABLE class

TABLE Vendors {
    vendor_id INT PK
    vendor_name VARCHAR(255)
}

TABLE Issuers {
    issuer_id INT PK
    issuer_name VARCHAR(255)
}

TABLE Runs {
    run_id INT PK
    run_date DATE
}

TABLE Indicators {
    indicator_id INT PK
    indicator_name VARCHAR(255)
}

TABLE Run_Indicators {
    run_id INT FK
    indicator_id INT FK
    PRIMARY KEY (run_id, indicator_id)
}

TABLE Indicator_Values {
    indicator_value_id INT PK
    indicator_id INT FK
    run_id INT FK
    issuer_id INT FK
    vendor_id INT FK
    value_date DATE
    value DECIMAL(10, 2)
    UNIQUE (indicator_id, run_id, vendor_id, value_date)
}

TABLE Attributes {
    attribute_id INT PK
    attribute_name VARCHAR(255)
    description TEXT
}

TABLE Indicator_Attributes {
    indicator_id INT FK
    attribute_id INT FK
    PRIMARY KEY (indicator_id, attribute_id)
}

TABLE Attribute_Values {
    attribute_value_id INT PK
    attribute_id INT FK
    run_id INT FK
    issuer_id INT FK
    value DECIMAL(10, 2)
    UNIQUE (attribute_id, run_id, issuer_id)
}

TABLE Alternative_Indicators {
    indicator_id INT FK
    alternative_indicator_id INT FK
    PRIMARY KEY (indicator_id, alternative_indicator_id)
}

-- Relationships
Runs ||--o{ Run_Indicators : "Has indicators"
Indicators ||--o{ Run_Indicators : "Is associated with a run"
Indicators ||--o{ Indicator_Values : "Has values for a specific run and vendor"
Vendors ||--o{ Indicator_Values : "Provides values"
Runs ||--o{ Indicator_Values : "Values associated with a run"
Issuers ||--o{ Indicator_Values : "Issuer responsible for value"
Attributes ||--o{ Attribute_Values : "Has attribute values for runs"
Issuers ||--o{ Attribute_Values : "Issuer responsible for attribute value"
Attributes ||--o{ Indicator_Attributes : "Is associated with many indicators"
Indicators ||--o{ Indicator_Attributes : "Has many attributes"
Indicators ||--o{ Alternative_Indicators : "Is alternative to"
@enduml
```

---

## Table Descriptions

### `Vendors`
- **vendor_id**: Unique identifier for each vendor.
- **vendor_name**: Name of the vendor providing the indicator data.

### `Issuers`
- **issuer_id**: Unique identifier for each issuer.
- **issuer_name**: Name of the issuer responsible for managing the runs.

### `Runs`
- **run_id**: Unique identifier for each run.
- **run_date**: The date on which the run occurs.

### `Indicators`
- **indicator_id**: Unique identifier for each indicator (e.g., temperature, humidity).
- **indicator_name**: Name of the indicator.

### `Run_Indicators`
- **run_id**: Foreign key referencing a specific run.
- **indicator_id**: Foreign key referencing a specific indicator.

### `Indicator_Values`
- **indicator_value_id**: Unique identifier for each recorded value.
- **indicator_id**: Foreign key referencing the indicator.
- **run_id**: Foreign key referencing the run.
- **issuer_id**: Foreign key referencing the issuer responsible for the indicator value.
- **vendor_id**: Foreign key referencing the vendor providing the indicator value.
- **value_date**: The date the value was recorded.
- **value**: The recorded value of the indicator.

### `Attributes`
- **attribute_id**: Unique identifier for each attribute.
- **attribute_name**: Name of the attribute (e.g., Max Temperature).
- **description**: Description of the attribute.

### `Indicator_Attributes`
- **indicator_id**: Foreign key referencing the indicator.
- **attribute_id**: Foreign key referencing the attribute.

### `Attribute_Values`
- **attribute_value_id**: Unique identifier for each recorded attribute value.
- **attribute_id**: Foreign key referencing the attribute.
- **run_id**: Foreign key referencing the run.
-

 **issuer_id**: Foreign key referencing the issuer responsible for the attribute value.
- **value**: The recorded value for the attribute.

### `Alternative_Indicators`
- **indicator_id**: Foreign key referencing the indicator.
- **alternative_indicator_id**: Foreign key referencing the alternative indicator.

---

## Example SQL Queries

Here are some useful SQL queries to interact with the database:

### 1. Insert Data

```sql
-- Insert an issuer
INSERT INTO Issuers (issuer_name) VALUES ('GlobalWeather');

-- Insert a vendor
INSERT INTO Vendors (vendor_name) VALUES ('WeatherCorp');

-- Insert an indicator
INSERT INTO Indicators (indicator_name) VALUES ('Temperature');

-- Insert a run
INSERT INTO Runs (run_date) VALUES ('2024-09-11');

-- Insert a run-indicator relationship
INSERT INTO Run_Indicators (run_id, indicator_id) VALUES (1, 1);

-- Insert an indicator value for a run, vendor, and issuer
INSERT INTO Indicator_Values (indicator_id, run_id, issuer_id, vendor_id, value_date, value)
VALUES (1, 1, 1, 1, '2024-09-11', 23.5);
```

### 2. Query Data

#### Retrieve all indicator values for a specific run:

```sql
SELECT i.indicator_name, iv.value, iv.value_date, v.vendor_name
FROM Indicator_Values iv
JOIN Indicators i ON iv.indicator_id = i.indicator_id
JOIN Vendors v ON iv.vendor_id = v.vendor_id
WHERE iv.run_id = 1;
```

#### Retrieve attributes and their values for a specific run:

```sql
SELECT a.attribute_name, av.value
FROM Attribute_Values av
JOIN Attributes a ON av.attribute_id = a.attribute_id
WHERE av.run_id = 1;
```

#### Retrieve alternative indicators for a specific indicator:

```sql
SELECT i.indicator_name, ai.alternative_indicator_id
FROM Alternative_Indicators ai
JOIN Indicators i ON ai.indicator_id = i.indicator_id
WHERE ai.indicator_id = 1;
```

---
