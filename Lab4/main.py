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
        return
    except:
        print("It is problem with connection to database")

    # your code here

def db_close():
    # Make the changes to the database persistent
    conn.commit()

    # Close communication with the database
    cursor.close()
    conn.close()

def main():

    file_list = find_files(root_dir, file_type)
    print(file_list)
    script_list = find_files(db_scripts_dir, db_scripts_type)
    print(script_list)

    check_scripts(file_list, script_list)

    db = False
    db_init()

    for filename in file_list:
        print(f"\nProcessing file <{filename}>...")

        #cursor.execute("DROP TABLE IF EXISTS %s", basename)

        with open(filename, "r") as f:
            csv_reader = list(csv.reader(f))[1:]
            #csv_reader = csv.reader(f)
            print(csv_reader)
            #for row in csv_reader:
            #    print(', '.join(row))

    if db:
        db_close()
    print("\nSuccess")

if __name__ == "__main__":
    main()
