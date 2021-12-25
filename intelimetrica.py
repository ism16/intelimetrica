import csv


def read_csv(file_name):
    """
    Read a csv file and returns a list containing its lines.
    """
    with open(file_name, 'r') as csv_file:
        reader = csv.reader(csv_file)
        return list(reader)


def read_to_list(file_name):
    """
    Reads a non-binary file and returns a list its containing lines.
    """
    with open(file_name, 'r') as file:
        file_list = file.readlines()
    return file_list


def write_to_psv(file_name, data):
    """
    Writes a list to a psv file.
    """
    with open(file_name, 'w', encoding='utf-8', newline='') as psv_file:
        writer = csv.writer(psv_file, delimiter='|')
        writer.writerows(data)


if __name__ == '__main__':
    # original_content = read_csv('starts_data.csv')
    # indexes = original_content[1][0].split('~') # 37

    original_content = read_to_list('starts_data.csv')
    clean_content = []
    deleted_lines = []

    for el in original_content:
        if el.find('~') != -1:
            clean_content.append(el.strip("\n").split('~'))
        elif el.find(',') != -1:
            clean_content.append(el.strip("\n").split(','))
        elif el.find('\t') != -1:
            clean_content.append(el.strip("\n").split('\t'))
        else:
            deleted_lines.append(el)

    # [el for el in clean_content if len(el) != 37]
    indexes = clean_content[0]
    write_to_psv('example.psv', clean_content)

