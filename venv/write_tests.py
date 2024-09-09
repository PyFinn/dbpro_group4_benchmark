import duckdb
import pandas as pd
import string
import random
import timeit
from memory_profiler import profile
import plot
from tpch_writing import convert_table_to_xlsx, create_versions_of_customer


def random_yes_no():
    return random.choice(["yes", "no"])


def generate_random_string(length):
    letters = string.ascii_lowercase
    random_string = ''.join(random.choice(letters) for i in range(length))
    return random_string


def random_int(min, max):
    return random.randint(min, max)


def mixed_data():
    a = random.randint(0, 5)
    if a == 0:
        return random_int(0, 100)
    if a == 1:
        return random_int(100, 1000)
    if a == 2:
        return random_int(1000, 10000)
    if a == 3:
        return random_yes_no()
    if a == 4:
        return generate_random_string(3)
    if a == 5:
        return generate_random_string(5)
    else:
        raise IndexError()


def create_data_frame(rows, cols):
    data = [[mixed_data() for _ in range(cols)] for _ in range(rows)]
    df = pd.DataFrame(data)
    return df


def create_data_frame2(rows, cols):
    data = [[generate_random_string(3) for _ in range(cols)] for _ in range(rows)]
    df = pd.DataFrame(data)
    return df


def create_file(dataframe, filename, db):
    con = duckdb.connect(f'{db}.db')
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    con.register('df', dataframe)
    con.execute('CREATE TABLE tbl AS SELECT * FROM df')
    con.execute(f"COPY (SELECT * FROM tbl) TO '{filename}.xlsx' WITH (FORMAT GDAL, DRIVER 'xlsx');")
    con.close()
    print("Written File!")


def test_file_sheetreader(filename, version=3, threads=1, flag=False):
    d = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    d.install_extension("./strv2/sheetreader.duckdb_extension",
                        force_install=True)
    d.load_extension("sheetreader")

    def f():
        d.sql(f'from sheetreader("{filename}",version={version},threads={threads},flag={flag})')

    def g():
        d.sql(f'create table tmp as from sheetreader("{filename}",version={version},threads={threads},flag={flag})')
        d.sql('drop table tmp')



    execution_time = timeit.timeit(g, number=1)
    d.close()
    return execution_time


def test_file_spatial(filename):
    duckdb.sql("INSTALL spatial;")
    duckdb.sql("LOAD spatial;")

    def exec():
        sql_command = f"create table tmp as FROM st_read('{filename}');"
        duckdb.sql(sql_command)
        duckdb.sql('drop table tmp')

    execution_time = timeit.timeit(exec, number=1)
    return execution_time


def memory_file(filename, version=3, threads=1, flag=False):
    duckdb.sql("INSTALL spatial;")
    duckdb.sql("LOAD spatial;")

    d = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    d.install_extension("/Users/finnklapper/PycharmProjects/dbpro/venv/strv2/sheetreader.duckdb_extension",
                        force_install=True)

    d.load_extension("sheetreader")

    def spatial():
        sql_command = f"create table tmp as FROM st_read('{filename}');"
        duckdb.sql(sql_command)
        duckdb.sql('drop table tmp')

    def sheetreader():
        d.sql(f'create table tmp as from sheetreader("{filename}",version={version},threads={threads},flag={flag})')
        d.sql('drop table tmp')

    @profile
    def process():
        spatial()
        sheetreader()

    process()


def write_output(filename, rows, time, func):
    with open(f"./{filename}.csv", "a") as f:
        f.write(str(rows) + f",{time}" + f",{func}\n")
        f.close()


def test_range(filename, start, end, step, output_filename, str_version=3, str_threads=[1]):
    counter = start
    while counter <= end:
        spatial_result = test_file_spatial(f"{filename}_{counter}.xlsx")
        write_output(output_filename, counter, spatial_result, "Spatial")
        for thread in str_threads:
            sheetreader_result = test_file_sheetreader(f"{filename}_{counter}.xlsx", version=str_version, threads=thread)
            write_output(output_filename, counter, sheetreader_result, f"SheetReader V{str_version} - {thread} Thread")

        counter *= step


def test_flag(filename, start, end, step, output_filename, str_version=3, str_threads=[1]):
    counter = start
    while counter <= end:
        spatial_result = test_file_spatial(f"{filename}_{counter}.xlsx")
        write_output(output_filename, counter, spatial_result, "Spatial")
        for thread in str_threads:
            sheetreader_result = test_file_sheetreader(f"{filename}_{counter}.xlsx", version=str_version,
                                                       threads=thread, flag=True)
            print(f"Sheet Reader - {thread} Thread - {counter} Rows")
            write_output(output_filename, counter, sheetreader_result, f"SheetReader V{str_version} - {thread} Thread")

        counter *= step

    a = input("Did you copy the Flag output into a file named 'flag_output.csv'? yN")
    if a == "y":
        plot.plot_flag_test("test_flag_output", "/Users/finnklapper/PycharmProjects/dbpro/dbpro_benchmarking/flag_output.csv", "Reading vs. Parsing")


def test_sheetreader_data_integrity(rows, cols, filename, version=3, threads=1, flag=False):
    df_original = create_data_frame(rows, cols)

    create_file(df_original, filename, "integrity")

    d = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    d.install_extension("./strv2/sheetreader.duckdb_extension", force_install=True)
    d.load_extension("sheetreader")

    df_read = d.sql(f"from sheetreader('{filename}.xlsx', threads={threads}, version={version}, flag={flag})").fetchdf()
    d.close()

    df_read.columns = df_read.iloc[0]
    df_read = df_read[1:]
    df_read.reset_index(drop=True, inplace=True)

    def convert_to_int(val):
        try:
            return int(val)
        except ValueError:
            return val

    for col in df_read.columns:
        df_read[col] = df_read[col].apply(convert_to_int)

    for i in range(len(df_original)):
        for j in range(len(df_original.columns)):
            if df_original.iat[i, j] != df_read.iat[i, j]:
                print(
                    f"Difference found at row {i}, column {j}: Original = {df_original.iat[i, j]}, Read = {df_read.iat[i, j]}")
                return
    print("Data integrity check passed: The dataframes are equal.")


def test_sheetreader_db_integrity(filename, db):
    convert_table_to_xlsx(db, 'customer', filename)

    d = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    d.install_extension("./strv2/sheetreader.duckdb_extension", force_install=True)
    d.load_extension("sheetreader")

    df_read = d.sql(f"from sheetreader('{filename}.xlsx')").fetchdf()
    d.close()

    con_read = duckdb.connect(f'{filename}_sheetreader.db')
    con_read.execute(f"CREATE TABLE customer AS SELECT * FROM df_read")
    con_read.close()

    con_original = duckdb.connect(f'{db}.db')
    con_read = duckdb.connect(f'{filename}_sheetreader.db')

    df_original = con_original.execute('SELECT * FROM customer').fetchdf()
    df_read = con_read.execute('SELECT * FROM customer').fetchdf()

    con_original.close()
    con_read.close()

    if df_original.equals(df_read):
        print("Data integrity check passed: The databases are equal.")
    else:
        print("Data integrity check failed: The databases are not equal.")
        print("Original DataFrame:")
        print(df_original)
        print("Read DataFrame:")
        print(df_read)


def test_customer_versions_to_xlsx(new_db_name):
    #create_versions_of_customer('tpch', new_db_name)

    #convert_table_to_xlsx(new_db_name, 'customer8', f'{new_db_name}_customer8')
    #convert_table_to_xlsx(new_db_name, 'customer6', f'{new_db_name}_customer6')
    #convert_table_to_xlsx(new_db_name, 'customer4', f'{new_db_name}_customer4')
    #convert_table_to_xlsx(new_db_name, 'customer2', f'{new_db_name}_customer2')
    #convert_table_to_xlsx(new_db_name, 'customer1', f'{new_db_name}_customer1')

    results = []

    for version in ['customer8','customer6', 'customer4', 'customer2', 'customer1']:
        file_name = f'{new_db_name}_{version}.xlsx'
        spatial_time = test_file_spatial(file_name)
        results.append([version, 'Spatial', spatial_time])

        for threads in [1, 2]:
            sheetreader_time = test_file_sheetreader(file_name, threads=threads)
            results.append([version, f'SheetReader {threads} Threads', sheetreader_time])

    with open(f'{new_db_name}_results.csv', 'w') as f:
        f.write('Version,Method,Time\n')
        for result in results:
            f.write(','.join(map(str, result)) + '\n')


if __name__ == '__main__':
    #test_range("./tpch_string_vs_int/consumer_strings", 100, 10000000, 10, "string_vs_int_output", str_threads=[1,2])
    #test_flag("./tpch_customer/tpch_customer", 100, 1000000, 10, "./test_flag_output", str_threads=[1, 2])
    #test_sheetreader_data_integrity(1000000, 10, './test_sheetreader')
    #test_sheetreader_db_integrity('./test_sheetreader', 'integrity_tpch')

    #test_customer_versions_to_xlsx('new_tpch')
    #memory_file("/Users/finnklapper/PycharmProjects/dbpro/venv/new_tpch_customer8.xlsx", threads=1)
    test_file_sheetreader("/Users/finnklapper/PycharmProjects/dbpro/venv/tpch_string_vs_int/consumer_ints_10000000.xlsx", flag=True)
