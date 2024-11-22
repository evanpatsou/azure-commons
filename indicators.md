```python
import pandas as pd
import numpy as np

# Sample df_left
df_left = pd.DataFrame({
    'id1': ['001', '002', '003', '004'],
    'id2': ['A1', 'A2', 'A3', 'A4'],
    'A': [10, np.nan, 30, np.nan],
    'B': [np.nan, 20, np.nan, 40],
    'C': ['foo', None, 'bar', None],
    'D': [100, 200, 300, 400]  # Column not in df_right
})

# Sample df_right
df_right = pd.DataFrame({
    'id1': ['002', '003', '005'],
    'id2': ['A2', 'A3', 'A5'],
    'A': [25, 35, 45],
    'B': [50, 55, 65],
    'C': [None, 'baz', 'qux'],
    'E': [500, 600, 700]  # Column not in df_left
})
```

### **Identify Common Columns**

```python
# Identify common columns (excluding 'id1' and 'id2')
common_columns = list(set(df_left.columns) & set(df_right.columns) - set(['id1', 'id2']))

print("Common columns to fill:", common_columns)
```

**Output:**

```
Common columns to fill: ['A', 'B', 'C']
```

### **Merge and Fill Missing Values**

```python
# Merge on 'id1'
df_merged_id1 = df_left.merge(
    df_right,
    on='id1',
    how='left',
    suffixes=('', '_right_id1')
)

# Merge on 'id2'
df_merged_id2 = df_left.merge(
    df_right,
    on='id2',
    how='left',
    suffixes=('', '_right_id2')
)

# Create a copy of df_left
df_filled = df_left.copy()

# Fill missing values in common columns
for col in common_columns:
    fill_from_id1 = df_merged_id1.get(f'{col}_right_id1')
    fill_from_id2 = df_merged_id2.get(f'{col}_right_id2')
    combined_fill = fill_from_id1.combine_first(fill_from_id2)
    df_filled[col] = df_filled[col].fillna(combined_fill)

# Display the filled DataFrame
print("\nFilled df_left:")
print(df_filled)
```

**Output:**

```
Filled df_left:
   id1 id2     A     B     C    D
0  001  A1  10.0   NaN   foo  100
1  002  A2  25.0  20.0  None  200
2  003  A3  30.0  55.0   bar  300
3  004  A4   NaN  40.0  None  400
```

### **Explanation:**

- **Row 0 (`id1=001`, `id2=A1`):**
  - No match in `df_right` on `id1` or `id2`.
  - Values remain as in `df_left`.

- **Row 1 (`id1=002`, `id2=A2`):**
  - Match on `id1=002` and `id2=A2` in `df_right`.
  - Missing value in `A` filled with `25.0` from `df_right`.
  - Column `B` remains as `20.0` from `df_left` (not missing).
  - `C` remains `None` (no value to fill from `df_right`).

- **Row 2 (`id1=003`, `id2=A3`):**
  - Match on `id1=003` and `id2=A3` in `df_right`.
  - Missing value in `B` filled with `55.0` from `df_right`.
  - `C` remains as `'bar'` from `df_left` (not missing).

- **Row 3 (`id1=004`, `id2=A4`):**
  - No match in `df_right` on `id1` or `id2`.
  - Values remain as in `df_left`.

---

## **Creating a Function for Reusability**

To make this process reusable, you can encapsulate the logic into a function.

```python
def fill_missing_values(df_left, df_right, id_cols):
    """
    Fill missing values in df_left using df_right based on matching id_cols.
    Fills all common columns between df_left and df_right (excluding id_cols).

    Parameters:
    - df_left: DataFrame to fill missing values in.
    - df_right: DataFrame to use for filling missing values.
    - id_cols: List of identifier columns to match on.

    Returns:
    - df_filled: DataFrame with missing values filled.
    """
    # Identify common columns (excluding id_cols)
    common_columns = list(set(df_left.columns) & set(df_right.columns) - set(id_cols))

    # Create a copy of df_left
    df_filled = df_left.copy()

    # Iterate over identifier columns
    for id_col in id_cols:
        # Merge on current id_col
        df_merged = df_left.merge(
            df_right,
            on=id_col,
            how='left',
            suffixes=('', f'_right_{id_col}')
        )

        # Fill missing values in common columns
        for col in common_columns:
            fill_values = df_merged.get(f'{col}_right_{id_col}')
            df_filled[col] = df_filled[col].fillna(fill_values)

    return df_filled
```

### **Using the Function**

```python
# Define the identifier columns
id_cols = ['id1', 'id2']

# Fill missing values
df_filled = fill_missing_values(df_left, df_right, id_cols)

# Display the filled DataFrame
print("\nFilled df_left using the function:")
print(df_filled)
```

**Output:**

```
Filled df_left using the function:
   id1 id2     A     B     C    D
0  001  A1  10.0   NaN   foo  100
1  002  A2  25.0  20.0  None  200
2  003  A3  30.0  55.0   bar  300
3  004  A4   NaN  40.0  None  400
```
