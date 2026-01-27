import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd


input_file = "C:\\Users\\benjamin.annan\\Downloads\\FullKeyInfo.parquet"
output_file = "updated_output.parquet"


try:
    table = pq.read_table(input_file)
    df = table.to_pandas()
    print("Parquet file loaded successfully.")

except Exception as e:
    print("Failed to read parquet file:")
    print(e)
    raise SystemExit


print("\nSchema:")
print(table.schema)

print("\nFirst 10 rows:")
print(df.head(10))

print("\nRow count:", len(df))
print("Columns:", list(df.columns))

#clean_table = pa.Table.from_pandas(df, preserve_index=False)


 