import pandas as pd

# Sample data
df_left = pd.DataFrame({
    'id1': ['A', 'B', 'C'],
    'id2': ['D', 'E', 'F'],
    'A': [10, None, 30],
    'B': [None, 20, None]
})

df_right = pd.DataFrame({
    'id1': ['B', 'E', 'X'],
    'id2': ['G', 'H', 'C'],
    'A': [25, 35, 45],
    'B': [45, 55, 65]
})

# Add an index to keep track of original rows
df_left = df_left.reset_index().rename(columns={'index': 'orig_index'})
df_right = df_right.reset_index().rename(columns={'index': 'orig_index'})

# Melt df_left
df_left_melted = df_left.melt(
    id_vars=[col for col in df_left.columns if col not in ['id1', 'id2']],
    value_vars=['id1', 'id2'],
    var_name='id_type',
    value_name='id'
)

# Melt df_right
df_right_melted = df_right.melt(
    id_vars=[col for col in df_right.columns if col not in ['id1', 'id2']],
    value_vars=['id1', 'id2'],
    var_name='id_type',
    value_name='id'
)

# Merge on 'id'
df_merged = df_left_melted.merge(
    df_right_melted,
    on='id',
    how='left',
    suffixes=('_left', '_right')
)

# Fill missing values
for col in ['A', 'B']:
    df_merged[col] = df_merged[f'{col}_left'].combine_first(df_merged[f'{col}_right'])

# Remove duplicates and restore original structure
df_filled = df_merged.sort_values('id').groupby('orig_index').first().reset_index()

# Select relevant columns
columns_to_keep = ['id1_left', 'id2_left', 'A', 'B']
df_filled = df_filled[columns_to_keep]

# Rename columns back to original names
df_filled.rename(columns={'id1_left': 'id1', 'id2_left': 'id2'}, inplace=True)

print(df_filled)
