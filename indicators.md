```python
def fill_missing_values(df_left, df_right, id_cols):
    """
    Update df_left with values from df_right, matching on id_cols.
    Prioritize df_right values to overwrite existing values in df_left.

    Parameters:
    - df_left: DataFrame to be updated.
    - df_right: DataFrame to update from.
    - id_cols: List of identifiers to match on (e.g., ['id1', 'id2']).

    Returns:
    - df_updated: Updated DataFrame.
    """
    # Identify all common columns (including id_cols)
    common_columns = list(set(df_left.columns) & set(df_right.columns))

    # Create a copy of df_left
    df_updated = df_left.copy()

    for id_col in id_cols:
        # Ensure identifiers are of the same type
        df_left[id_col] = df_left[id_col].astype(str)
        df_right[id_col] = df_right[id_col].astype(str)

        # Merge df_updated with df_right on id_col
        df_merged = df_updated.merge(
            df_right,
            on=id_col,
            how='left',
            suffixes=('', '_right')
        )

        # For each common column, update df_updated with df_right values
        for col in common_columns:
            col_right = f'{col}_right'
            if col_right in df_merged.columns:
                # Update df_updated[col] with df_right values where not null
                df_updated[col] = df_merged[col_right].combine_first(df_updated[col])

    return df_updated
```
