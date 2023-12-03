import glob
import json
from flatten_json import flatten
#import csv

root_dir = 'data'
file_type = 'json'

def main():
    file_list = glob.glob(root_dir + '/**/*.' + file_type, recursive=True)
    print(file_list)

    for file_path in file_list:
        print()
        print(file_path)
        with open(file_path, "r") as json_file:
            json_data = json.load(json_file)
            #print(data)
            flattened_data = flatten(json_data, "_")
            #print(flattened_data)

            csv_filename = file_path[:-len(file_type)] + 'csv'
            print(csv_filename)

            keys = []
            row = []
            for key, value in flattened_data.items():
                keys.append(str(key))
                row.append(str(value))
            print(keys)
            print(row)

            with open(csv_filename, 'w', newline='') as csv_file:
                csv_file.write('sep =,')
                csv_file.write('\n')
                csv_file.write(",".join(keys))
                csv_file.write('\n')
                csv_file.write(",".join(row))

if __name__ == "__main__":
    main()
