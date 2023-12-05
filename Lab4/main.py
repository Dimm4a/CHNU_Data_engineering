import psycopg2
import glob
import csv

root_dir = 'data'
file_type = 'csv'
db_scripts_dir = 'db_scripts'
db_scripts_type = 'db_script'

def find_files(dir, type):
    return glob.glob(dir + '/**/*.' + type, recursive=True)

def base_name(path):
    basename = path.split('.')[0]
    basename = basename.split('\\')[-1]
    return basename

def check_scripts(file_list, script_list):
    print()
    changes = False
    i = -1
    for file_path in file_list:
        file_basename = base_name(file_path)
        i += 1
        match = False
        for script_path in script_list:
            script_basename = base_name(script_path)
            if script_basename == file_basename:
                match = True
                break
        if not match:
            changes = True
            file_list.pop(i)
            print(f"There is no script for file <{file_path}>. It was removed from list")
    if changes:
        print(f"Final file list to processing:\n{file_list}")

def db_init():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"

    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
        cursor = conn.cursor()
        db = True
        return cursor, conn
    except:
        print("\nIt is problem with connection to database")
        return None, None

def clear_db_data(file_list, cursor):
    for file_path in file_list:
        table_name = base_name(file_path)
        cursor.execute('DROP TABLE IF EXISTS %s', table_name)

def db_close(conn, cursor):
    # Make the changes to the database persistent
    conn.commit()

    # Close communication with the database
    cursor.close()
    conn.close()

def execute_scripts(script_list, cursor):
    for script_path in script_list:
        with open(script_path, 'r') as file:
            print(f"Executing script: <{script_path}>...")
            db_script = file.read()
            cursor.execute(db_script)
            print("Done")
    print("Tables are created")

def check_data(file_list, cursor):
    for file_path in file_list:
        table_name = base_name(file_path)
        print(f"\nData from table in db: {table_name}")
        cursor.execute('SELECT * FROM %s', table_name)
        print(cursor.fetchall())

def processing_files(file_list, cursor):
    for file_path in file_list:
        table_name = base_name(file_path)
        print(f"\nProcessing file <{file_path}>...")
        with open(file_path, "r") as file:
            csv_reader = csv.reader(file)
            data = list(csv_reader)
            header_array = data[0]
            table_data_array = data[1:]
            #print(header_array)
            #print(table_data_array)

            header = ', '.join(header_array)
            print(header)

            for row in table_data_array:
                row_data = ', '.join(row)
                print(row_data)
                if cursor:
                    cursor.executemany('''
                        INSERT INTO %s (%s)
                        VALUES (%s)''', (table_name, (header,), (row_data,)))

def main():

    file_list = find_files(root_dir, file_type)
    print(file_list)
    script_list = find_files(db_scripts_dir, db_scripts_type)
    print(script_list)

    check_scripts(file_list, script_list)

    db = False
    cursor, conn = db_init()
    if db:
        clear_db_data(file_list, cursor)
        execute_scripts(script_list, cursor)

    processing_files(file_list, cursor)

    if db:
        check_data(file_list, cursor)
        clear_db_data(file_list, cursor)
        db_close(conn, cursor)
        print("\nSuccess")
    else:
        print("\nThe end")


if __name__ == "__main__":
    main()
