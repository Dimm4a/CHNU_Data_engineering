import psycopg2
import glob
import csv

root_dir = 'data'
file_type = 'csv'


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    # your code here

    cursor = conn.cursor()

def main():

    file_list = glob.glob(root_dir + '/**/*.' + file_type, recursive=True)
    print(file_list)

    for filename in file_list:
        basename = filename.split('.')[0]
        basename = basename.split('\\')[-1]
        print(basename)

        with open(filename, "r") as f:
            csv_reader = list(csv.reader(f))[0]
            #csv_reader = csv.reader(f)
            print()
            print(csv_reader)
            #for row in csv_reader:
            #    print(', '.join(row))


if __name__ == "__main__":
    main()
