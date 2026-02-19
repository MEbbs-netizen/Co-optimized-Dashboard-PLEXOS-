import os
import glob
import pyarrow.parquet as pq
PARQUET_DIR = r"C:\\Users\\benjamin.annan\\Downloads\\Insights FBMC Op Jan08 - 10 samples WO LA_ParquetFile 1\\Insights FBMC Op Jan08 - 10 samples WO LA_ParquetFile" #replace path here
def check_parquet_with_pyarrow(path: str):
   try:
       pq_file = pq.ParquetFile(path)
       print(f"OK: {path} ({pq_file.metadata.num_rows} rows, {pq_file.metadata.num_columns} columns)")
   except Exception as e:
       print(f"BAD: {path} ({e})")
def main():
   files = glob.glob(os.path.join(PARQUET_DIR, "**", "*.parquet"), recursive=True)
   if not files:
       print("No parquet files found.")
       return
   for path in files:
       check_parquet_with_pyarrow(path)
if __name__ == "__main__":
   main()