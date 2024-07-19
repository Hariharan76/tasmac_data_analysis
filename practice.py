import pandas as pd

# Example DataFrame
df = pd.DataFrame({
    'Column1': ['A', 'B', 'C'],
    'Column2': ['X', 'Y', 'Z']
})

# Combine Column1 and Column2 and convert to a list
combined_list = df['Column1'].tolist() + df['Column2'].tolist()

print(combined_list)
