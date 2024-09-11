### Optimized SQL Schema Code

To optimize the SQL schema, we can ensure that:

1. **Indexes**: Add indexes to frequently queried columns, such as foreign keys, to speed up queries.
2. **Data Integrity**: Ensure proper constraints (such as `NOT NULL`, `UNIQUE`, etc.) for data consistency.
3. **Naming Conventions**: Use clear and consistent naming conventions for tables, columns, and constraints.

Here’s the optimized SQL schema:

```sql
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
    issuer_id INT NOT NULL REFERENCES Issuers(issuer_id) ON DELETE CASCADE,
    run_date DATE NOT NULL,
    UNIQUE (issuer_id, run_date) -- Ensures a run by an issuer on a given date is unique
);

-- Indicators Table
CREATE TABLE Indicators (
    indicator_id SERIAL PRIMARY KEY,
    indicator_name VARCHAR(255) NOT NULL,
    UNIQUE (indicator_name) -- Ensure indicator names are unique
);

-- Run_Indicators Table
CREATE TABLE Run_Indicators (
    run_id INT NOT NULL REFERENCES Runs(run_id) ON DELETE CASCADE,
    indicator_id INT NOT NULL REFERENCES Indicators(indicator_id) ON DELETE CASCADE,
    vendor_id INT NOT NULL REFERENCES Vendors(vendor_id) ON DELETE CASCADE,
    PRIMARY KEY (run_id, indicator_id, vendor_id)
);

-- Indicator_Values Table
CREATE TABLE Indicator_Values (
    indicator_value_id SERIAL PRIMARY KEY,
    indicator_id INT NOT NULL REFERENCES Indicators(indicator_id) ON DELETE CASCADE,
    value_date DATE NOT NULL,
    value DECIMAL(10, 2) NOT NULL,
    UNIQUE (indicator_id, value_date) -- Ensure no duplicate values for the same indicator on a date
);

-- Attributes Table
CREATE TABLE Attributes (
    attribute_id SERIAL PRIMARY KEY,
    attribute_name VARCHAR(255) NOT NULL,
    description TEXT,
    indicator_id INT NOT NULL REFERENCES Indicators(indicator_id) ON DELETE CASCADE,
    UNIQUE (attribute_name, indicator_id) -- Ensure each attribute name is unique per indicator
);

-- Attribute_Values Table
CREATE TABLE Attribute_Values (
    attribute_value_id SERIAL PRIMARY KEY,
    attribute_id INT NOT NULL REFERENCES Attributes(attribute_id) ON DELETE CASCADE,
    run_id INT NOT NULL REFERENCES Runs(run_id) ON DELETE CASCADE,
    value DECIMAL(10, 2) NOT NULL,
    UNIQUE (attribute_id, run_id) -- Prevent duplicate attribute values for the same run
);

-- Alternative_Indicators Table
CREATE TABLE Alternative_Indicators (
    indicator_id INT NOT NULL REFERENCES Indicators(indicator_id) ON DELETE CASCADE,
    alternative_indicator_id INT NOT NULL REFERENCES Indicators(indicator_id) ON DELETE CASCADE,
    PRIMARY KEY (indicator_id, alternative_indicator_id)
);
```

### Optimizations:
1. **`UNIQUE` Constraints**: I added `UNIQUE` constraints on relevant columns like `indicator_name`, `attribute_name` per `indicator_id`, `run_date` per `issuer_id`, and value uniqueness per date to avoid duplicates.
2. **`ON DELETE CASCADE`**: If an `Issuer`, `Run`, `Indicator`, or `Vendor` is deleted, all associated data (like runs or values) will be removed automatically.
3. **Indexes**: By default, primary keys and foreign keys create indexes, which will optimize your queries. If more performance is needed, custom indexing can be added based on usage patterns.

### Common Queries You Might Need

#### 1. **Insert Data**
Here’s how to insert basic data into the tables.

##### Insert Issuer, Run, Vendor, Indicator:
```sql
-- Insert into Issuers
INSERT INTO Issuers (issuer_name) VALUES ('GlobalWeather');

-- Insert into Vendors
INSERT INTO Vendors (vendor_name) VALUES ('WeatherCorp');

-- Insert into Indicators
INSERT INTO Indicators (indicator_name) VALUES ('Temperature');

-- Insert into Runs
INSERT INTO Runs (issuer_id, run_date) VALUES (1, '2024-09-11');
```

#### 2. **Associate Indicators with Runs and Vendors**
```sql
-- Associate a run with an indicator and vendor
INSERT INTO Run_Indicators (run_id, indicator_id, vendor_id)
VALUES (1, 1, 1); -- (run_id = 1, indicator_id = 1, vendor_id = 1)
```

#### 3. **Insert Attribute and Attribute Values**
```sql
-- Insert attributes for an indicator
INSERT INTO Attributes (attribute_name, description, indicator_id)
VALUES ('Max Temperature', 'Maximum temperature during the day', 1),
       ('Min Temperature', 'Minimum temperature during the day', 1);

-- Insert attribute values for a run
INSERT INTO Attribute_Values (attribute_id, run_id, value)
VALUES (1, 1, 32.5), -- Max Temperature value
       (2, 1, 15.2); -- Min Temperature value
```

#### 4. **Insert Indicator Values**
```sql
-- Insert indicator values
INSERT INTO Indicator_Values (indicator_id, value_date, value)
VALUES (1, '2024-09-11', 25.3);
```

#### 5. **Retrieve Runs and Their Associated Indicators**

To find all indicators tracked in a specific run:

```sql
SELECT r.run_id, r.run_date, i.indicator_name, v.vendor_name
FROM Runs r
JOIN Run_Indicators ri ON r.run_id = ri.run_id
JOIN Indicators i ON ri.indicator_id = i.indicator_id
JOIN Vendors v ON ri.vendor_id = v.vendor_id
WHERE r.run_id = 1;
```

#### 6. **Retrieve Attribute Values for a Run**

To get all attribute values (such as Max and Min Temperature) recorded for a specific run:

```sql
SELECT r.run_date, a.attribute_name, av.value
FROM Runs r
JOIN Attribute_Values av ON r.run_id = av.run_id
JOIN Attributes a ON av.attribute_id = a.attribute_id
WHERE r.run_id = 1;
```

#### 7. **Retrieve Indicator Values Over Time**

To track an indicator’s values over time (e.g., Temperature):

```sql
SELECT i.indicator_name, iv.value_date, iv.value
FROM Indicators i
JOIN Indicator_Values iv ON i.indicator_id = iv.indicator_id
WHERE i.indicator_id = 1
ORDER BY iv.value_date;
```

#### 8. **Get Alternative Indicators for a Given Indicator**

If you want to find alternative indicators for a given indicator:

```sql
SELECT i1.indicator_name AS main_indicator, i2.indicator_name AS alternative_indicator
FROM Alternative_Indicators ai
JOIN Indicators i1 ON ai.indicator_id = i1.indicator_id
JOIN Indicators i2 ON ai.alternative_indicator_id = i2.indicator_id
WHERE i1.indicator_id = 1;
```

#### 9. **Get Vendor Providing Data for a Specific Indicator in a Run**

If you need to know which vendor provided the data for a specific indicator during a specific run:

```sql
SELECT v.vendor_name, i.indicator_name, r.run_date
FROM Vendors v
JOIN Run_Indicators ri ON v.vendor_id = ri.vendor_id
JOIN Indicators i ON ri.indicator_id = i.indicator_id
JOIN Runs r ON ri.run_id = r.run_id
WHERE r.run_id = 1 AND i.indicator_id = 1;
```

### Index Optimization

For more performance optimization, we can manually add some indexes to improve query speed when retrieving or filtering data:

```sql
-- Create indexes on foreign key columns for faster joins
CREATE INDEX idx_attribute_values_run_id ON Attribute_Values(run_id);
CREATE INDEX idx_attribute_values_attribute_id ON Attribute_Values(attribute_id);
CREATE INDEX idx_run_indicators_run_id ON Run_Indicators(run_id);
CREATE INDEX idx_run_indicators_indicator_id ON Run_Indicators(indicator_id);
CREATE INDEX idx_indicator_values_indicator_id ON Indicator_Values(indicator_id);
```

These indexes should help with speeding up queries involving joins or filtering by `run_id`, `indicator_id`, and `attribute_id`.

### Conclusion

This optimized schema and query set should give you a highly efficient way to store and query your data. You can adapt the queries to fit specific business logic or analysis needs. Let me know if you need further adjustments!