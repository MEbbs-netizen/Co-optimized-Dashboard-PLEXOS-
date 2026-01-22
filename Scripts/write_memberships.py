import duckdb
import os
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


def main():
    simulation_path = os.environ.get('simulation_path', "/simulation")
    output_path = os.environ.get('output_path', "/output") 

    database_file_path = os.path.join(simulation_path, "reference.db")
    memberships_file_path = os.path.join(output_path, "memberships_data.csv")

    try:
        with duckdb.connect() as con:
            con.execute("INSTALL sqlite;")
            con.execute("LOAD sqlite;")

            print(f"Attaching SQLite database: {database_file_path}")
            con.execute(f"ATTACH '{database_file_path}' (TYPE SQLITE);")
            con.execute("USE reference;")

            print("Running membership query...")
            query = """
                SELECT 
                    cl1.Name AS parent_class,
                    cl2.Name AS child_class,
                    col.Name AS collection,
                    obj1.Name AS parent_object,
                    obj2.Name AS child_object,
                    '' AS subcollection_name
                FROM t_membership mem 
                INNER JOIN t_object obj1 ON obj1.object_id = mem.parent_object_id
                INNER JOIN t_object obj2 ON obj2.object_id = mem.child_object_id
                INNER JOIN t_collection col ON col.collection_id = mem.collection_id
                INNER JOIN t_class cl1 ON cl1.class_id = mem.parent_class_id
                INNER JOIN t_class cl2 ON cl2.class_id = mem.child_class_id
            """

            os.makedirs(output_path, exist_ok=True)

            # UTF-8 encoding added here to fix Windows charmap issues
            con.execute(f"""
    COPY ({query}) TO '{memberships_file_path}' 
    (HEADER, DELIMITER ',');
""")


            print(f"Membership data written to: {memberships_file_path}")
            print("Sample output:")
            con.sql(query + " LIMIT 5").show()

    except Exception as e:
        print("Error while writing memberships CSV:")
        print(e)
    finally:
        print("done")

if __name__ == "__main__":
    main()
