import re


def get_column_name_from_file(filename):
    try:
        with open(filename, 'r') as data_file:
            coloumn_names = list(
                map(lambda x: re.sub(' ', '_', x.strip()),
                    data_file.readline().split(',')
                    )
            )
            print(coloumn_names)
            return coloumn_names
    except FileNotFoundError:
        print("\nPlease check the provided filename\n")
        exit()


def get_records_from_file(filename):
    try:
        data_list = []
        with open(filename, 'r') as data_file:
            # Skip the first line
            data = data_file.readline()
            data = data_file.readline()

            while(data):
                # Read the file and remove special characters from the file
                removed_special_characters = list(
                    map(lambda x: re.sub(
                        "[^a-zA-Z0-9,]", "", x),
                        data.split(',')
                        )
                )
                data = data_file.readline()
                # print(removed_special_characters)
                data_list.append(removed_special_characters)
        return(data_list)
    except FileNotFoundError:
        print("\nPlease check the provided filename\n")
        exit()


if __name__ == '__main__':

    filename = 'sample_data.txt'
    get_column_name_from_file(filename)
    get_records_from_file(filename)
