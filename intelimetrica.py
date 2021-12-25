import csv
from prefect import Flow, task
from typing import List, Tuple


def read_csv(file_name: str) -> List[List[str]]:
    """
    Read a csv file and returns a list containing its lines.
    """
    with open(file_name, 'r') as csv_file:
        reader = csv.reader(csv_file)
        return list(reader)


def read_to_list(file_name: str) -> List[List[str]]:
    """
    Reads a non-binary file and returns a list its containing lines.
    """
    with open(file_name, 'r') as file:
        file_list = file.readlines()
    return file_list


def write_to_psv(file_name: str, data: List[List[str]]) -> None:
    """
    Writes a list to a psv file.
    """
    with open(file_name, 'w', encoding='utf-8', newline='') as psv_file:
        writer = csv.writer(psv_file, delimiter='|')
        writer.writerows(data)


def clean_data(data: List[List[str]]) -> Tuple[List[List[str]], List[List[str]]]:
    """
    Cleans the data by removing the all lines not containing a separator.
    E.g. '~', ',', '\t'
    """
    clean_content = []
    deleted_lines = []

    for el in data:
        if el.find('~') != -1:
            clean_content.append(el.strip("\n").split('~'))
        elif el.find(',') != -1:
            clean_content.append(el.strip("\n").split(','))
        elif el.find('\t') != -1:
            clean_content.append(el.strip("\n").split('\t'))
        else:
            deleted_lines.append(el)
    
    return clean_content, deleted_lines


@task
def extract(file_name):
    return read_to_list(file_name)


@task
def transform(data):
    clean, _ = clean_data(data)
    return clean


@task
def load(file_name, data):
    write_to_psv(file_name, data)


if __name__ == '__main__':
    with Flow('clean_intelimetrica_data') as flow:
        data = extract('starts_data.csv')
        tdata = transform(data)
        load('starts_data.psv', tdata)

    flow.run()