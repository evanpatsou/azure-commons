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
    # Identify all common columns (excluding id_cols to prevent suffixes on them)
    common_columns = [col for col in df_left.columns if col in df_right.columns and col not in id_cols]

    # Ensure identifiers are of the same type
    df_left[id_cols] = df_left[id_cols].astype(str)
    df_right[id_cols] = df_right[id_cols].astype(str)

    # Merge df_left with df_right on id_cols
    df_merged = df_left.merge(
        df_right,
        on=id_cols,
        how='left',
        suffixes=('', '_right')
    )

    # Update df_left with df_right values where not null
    for col in common_columns:
        col_right = f'{col}_right'
        if col_right in df_merged.columns:
            df_merged[col] = df_merged[col_right].combine_first(df_merged[col])
            df_merged.drop(columns=[col_right], inplace=True)

    # Return the updated DataFrame
    df_updated = df_merged[df_left.columns]

    return df_updated!3
```
