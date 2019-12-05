import csv,os
from pathlib import Path

def read_answer(file_name):
    data_folder = "Tests/Data/" + file_name
    relative_file_path = Path(data_folder)
    absolute_file_path = relative_file_path.absolute()
    f = open(absolute_file_path, 'r')
    csv_reader = csv.reader(f, delimiter=',')
    test_row_list = list()
    for row in csv_reader:
        test_row_list.append(row)
    f.close()
    answer = 0
    for row in test_row_list:
        answer = round(float(row[0]), 2)

    return answer