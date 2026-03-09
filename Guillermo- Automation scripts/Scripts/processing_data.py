import os
import time
import duckdb
import datetime
from dotenv import load_dotenv
import logging
import os

APILogger = logging.getLogger("APILogger")
if not APILogger.handlers:
    APILogger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    APILogger.addHandler(sh)
    log_dir = os.getenv("output_path", "./output")
    os.makedirs(log_dir, exist_ok=True)
    fh = logging.FileHandler(os.path.join(log_dir, "GasModelcheck.log"), encoding="utf-8")
    fh.setFormatter(formatter)
    APILogger.addHandler(fh)


# Load environment variables
load_dotenv()

def load_memberships(con, output_path):
    memberships_path = os.path.join(output_path, "memberships_data.csv")
    if not os.path.exists(memberships_path):
        print(f"Warning: Memberships CSV not found at {memberships_path}. Skipping import.")
        return

    print(f"Loading memberships from {memberships_path}...")
    con.execute("DROP TABLE IF EXISTS memberships;")
    con.execute(f"""
        CREATE TABLE memberships AS 
        SELECT * FROM read_csv_auto('{memberships_path}', HEADER=TRUE);
    """)
    print("Memberships table loaded.")

def configure_views(con):
    """Define views using simulation and membership data."""
    con.execute("""
        CREATE OR REPLACE VIEW regional_generation_capacity AS
        SELECT
            key.*, 
            d.PeriodId, 
            d.Value
        FROM fullkeyinfo AS key
        INNER JOIN data d ON d.SeriesId = key.SeriesId
        WHERE
            key.PeriodTypeName = 'Interval'
            AND key.PhaseName = 'ST'
            AND key.ParentClassName = 'System'
            AND key.ChildClassName = 'Generator'
            AND key.PropertyName IN ('Generation', 'Available Capacity');
    """)

    con.execute("""
        CREATE OR REPLACE VIEW region_aggregate_totals AS
        SELECT
            PhaseName, BandId, PeriodTypeName, 
            ParentObjectCategoryName, ParentObjectName, ChildObjectCategoryName, 
            ParentClassName, CollectionName, ChildClassName, PropertyName, UnitValue, TimesliceName, ModelName, SampleId, SampleName, PeriodId,
            ChildObjectName,
            SUM(Value) AS TotalValue
        FROM regional_generation_capacity
        GROUP BY 
            PhaseName, BandId, PeriodTypeName, 
            ParentObjectCategoryName, ParentObjectName, ChildObjectCategoryName, 
            ParentClassName, CollectionName, ChildClassName, PropertyName, UnitValue, TimesliceName, ModelName, SampleId, SampleName, PeriodId, ChildObjectName;
    """)

    con.execute("""
        CREATE OR REPLACE VIEW reporting_data AS
        SELECT 
            t.PhaseName, t.BandId, t.PeriodTypeName, 
            t.ParentObjectCategoryName, t.ParentObjectName, t.ChildObjectCategoryName, 
            t.ParentClassName, t.CollectionName, t.ChildClassName, t.PropertyName, t.UnitValue, t.TimesliceName, 
            t.ModelName, t.SampleId, t.SampleName, 
            m.parent_class AS MembershipParentClass,
            m.parent_object AS MembershipParentObject,
            m.collection AS MembershipCollection,
            p.StartDate, p.EndDate, current_localtimestamp() AS SolutionDate, 
            t.TotalValue
        FROM region_aggregate_totals t
        LEFT JOIN memberships m 
            ON m.child_object = t.ChildObjectName AND m.child_class = t.ChildClassName
        INNER JOIN Period p 
            ON p.PeriodId = t.PeriodId;
    """)

def export_data(con, output_path, date_str):
    parquet_path = os.path.join(output_path, f"solution_data_{date_str}.parquet")
    csv_path = os.path.join(output_path, f"solution_data_{date_str}.csv")

    con.execute(f"""
        COPY (SELECT * FROM reporting_data)
        TO '{parquet_path}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000);
    """)
    print(f"Parquet export complete: {parquet_path}")

    con.execute(f"""
        COPY (SELECT * FROM reporting_data)
        TO '{csv_path}' (HEADER TRUE, DELIMITER ',');
    """)
    print(f"CSV export complete: {csv_path}")

def main():
    try:
        output_path = os.getenv('output_path') or './output'
        duckdb_path = os.getenv('duck_db_path') or os.path.join(output_path, 'solution_views.ddb')

        if not os.path.exists(duckdb_path):
            raise FileNotFoundError(f"DuckDB file not found: {duckdb_path}")

        today_str = datetime.date.today().strftime("%Y-%m-%d")
        os.makedirs(output_path, exist_ok=True)

        with duckdb.connect(duckdb_path) as con:
            start = time.time()

            load_memberships(con, output_path)
            configure_views(con)
            export_data(con, output_path, today_str)

            print(f"Processing completed in {time.time() - start:.2f} seconds")

    except Exception as e:
        print("Processing failed with exception:")
        print(e)
    finally:
        print("done")

if __name__ == "__main__":
    main()
