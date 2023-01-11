import pathlib
import csv

def tests_submission_csv_input():
    file_path = pathlib.Path('test_data/submission.csv')
    with file_path.open() as f:
        csv_reader = csv.reader(f)
        rows = list(csv_reader)
    assert(len(rows) == 3)
    for row in rows:
        assert(len(row) == 575)