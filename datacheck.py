import pandas as pd


df = pd.read_csv('/Users/staudacherld/Downloads/baselineforcostandretailpriceupdate.csv')
sku = 'ESH-G01-H510A-10'
if sku not in df['Variant SKU'].values:
    print(f"SKU {sku} not found in the file.")

# Filter the row corresponding to the SKU
sku_row = df[df['Variant SKU'] == sku]

# Transpose the row to get column headers and values
non_blank_data = sku_row.T  # Transpose
non_blank_data = non_blank_data[non_blank_data.iloc[:, 0].notnull()]  # Filter non-blank rows

# Create a DataFrame with column headers and values
result = pd.DataFrame({
    "Column Header": non_blank_data.index,
    "Value": non_blank_data.iloc[:, 0].values
})

print("something")