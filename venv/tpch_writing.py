import duckdb

def create_tpch_xlsx(output_file_name, scale_factor=1, tpch_table_name="customer"):
    con = duckdb.connect('tpch.db')
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    con.execute("INSTALL tpch;")
    con.execute("LOAD tpch;")

    con.execute(f"CALL dbgen(sf = {scale_factor});")

    all_cols = con.execute(f"DESCRIBE {tpch_table_name}").fetchall()
    decimal_columns = []

    for column in all_cols:
        column_name = column[0]
        column_type = column[1]

        if 'DECIMAL' in column_type:
            decimal_columns.append(column_name)

    for column in decimal_columns:
        col_name = column
        bigint_col_name = f"{col_name}_bigint"
        con.execute(f"ALTER TABLE {tpch_table_name} ADD COLUMN {bigint_col_name} BIGINT;")
        con.execute(f"UPDATE {tpch_table_name} SET {bigint_col_name} = CAST({col_name} AS BIGINT);")
        con.execute(f"ALTER TABLE {tpch_table_name} DROP COLUMN {col_name};")
        con.execute(f"ALTER TABLE {tpch_table_name} RENAME COLUMN {bigint_col_name} TO {col_name};")

    con.execute(f"COPY (SELECT * FROM {tpch_table_name}) TO '{output_file_name}.xlsx' WITH (FORMAT GDAL, DRIVER 'xlsx');")

    con.close()


def convert_table_to_xlsx(db_name, table_name, output_file_name):
    con = duckdb.connect(f'{db_name}.db')
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    all_cols = con.execute(f"DESCRIBE {table_name}").fetchall()
    decimal_columns = []

    for column in all_cols:
        column_name = column[0]
        column_type = column[1]

        if 'DECIMAL' in column_type:
            decimal_columns.append(column_name)

    for column in decimal_columns:
        col_name = column
        bigint_col_name = f"{col_name}_bigint"
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN {bigint_col_name} BIGINT;")
        con.execute(f"UPDATE {table_name} SET {bigint_col_name} = CAST({col_name} AS BIGINT);")
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN {col_name};")
        con.execute(f"ALTER TABLE {table_name} RENAME COLUMN {bigint_col_name} TO {col_name};")

    con.execute(f"COPY (SELECT * FROM {table_name}) TO '{output_file_name}.xlsx' WITH (FORMAT GDAL, DRIVER 'xlsx');")

    con.close()


def create_versions_of_customer(db_name, new_db_name):
    con = duckdb.connect(f'{db_name}.db')
    df_customer = con.execute('SELECT * FROM customer').fetchdf()
    con.close()

    con_new = duckdb.connect(f'{new_db_name}.db')
    con_new.execute('CREATE TABLE customer8 AS SELECT * FROM df_customer')

    all_cols = con_new.execute("DESCRIBE customer8").fetchall()
    column_names = [col[0] for col in all_cols]

    con_new.execute(
        f'CREATE TABLE customer6 AS SELECT {column_names[0]}, {column_names[1]}, {column_names[2]}, {column_names[3]}, {column_names[4]}, {column_names[5]} FROM customer8')

    con_new.execute(
        f'CREATE TABLE customer4 AS SELECT {column_names[0]}, {column_names[1]}, {column_names[2]}, {column_names[3]} FROM customer8')
    con_new.execute(f'CREATE TABLE customer2 AS SELECT {column_names[0]}, {column_names[1]} FROM customer8')
    con_new.execute(f'CREATE TABLE customer1 AS SELECT {column_names[0]} FROM customer8')
    con_new.close()


def convert_single_column_to_xlsx(db_name, table_name, output_file_name, column_index):
    con = duckdb.connect(f'{db_name}.db')
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    all_cols = con.execute(f"DESCRIBE {table_name}").fetchall()
    decimal_columns = []

    if column_index < 0 or column_index >= len(all_cols):
        raise IndexError("column_index out of range")

    column_name = all_cols[column_index][0]
    column_type = all_cols[column_index][1]

    if 'DECIMAL' in column_type:
        bigint_col_name = f"{column_name}_bigint"
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN {bigint_col_name} BIGINT;")
        con.execute(f"UPDATE {table_name} SET {bigint_col_name} = CAST({column_name} AS BIGINT);")
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name};")
        con.execute(f"ALTER TABLE {table_name} RENAME COLUMN {bigint_col_name} TO {column_name};")

    con.execute(f"COPY (SELECT {column_name} FROM {table_name} LIMIT 10000000) TO '{output_file_name}.xlsx' WITH (FORMAT GDAL, DRIVER 'xlsx');")

    con.close()

