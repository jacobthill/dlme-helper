#!/usr/bin/python
import glob, json, ndjson, os
from argparse import ArgumentParser
from collections import Counter


########## Inspection Helper Functions ##########


# Cluster field values
def cluster_fields(records):
    in_field_values = []
    out_field_values = []
    for record in records:
        if args.field_one in record:
            try:
                in_field_values.append(record[args.field_one][0])
            except:
                for i in record[args.field_one].items():
                    print(i[1])
                    in_field_values.append(i)
        if args.field_two in record:
            out_field_values.append(record[args.field_two][0])
    # Count each value in list
    print("-------------------------------")
    print("Clusters:")
    try:
        in_clusters = Counter(in_field_values).most_common() # Count and convert to sorted list of lists
        out_clusters = Counter(out_field_values).most_common() # Count and convert to sorted list of lists
    except:
        in_clusters = in_field_values
        out_clusters = out_field_values
    return in_clusters, out_clusters

########## Core Functions for each Stage ##########


# Inspect incoming values
def inspect(records, blank_lines, invalid_json):
    in_clusters = cluster_fields(records)[0]
    for item in in_clusters:
        print("{}: {}".format(item[0], item[1]))
    field_count = 0
    for record_count, record in enumerate(records, start=1):
        if args.field_one in record:
            field_count += 1

    print("\n")
    print("***************************** Field Mapping Summary Report *****************************")
    print("\n")
    print("{} of {} records have the {} value".format(field_count, record_count, args.field_one))
    print('---------------------------------------------------------------------------------------')

    print("\n")
    print("There were {} blank lines in the intermediate representation.".format(len(blank_lines)))
    print('---------------------------------------------------------------------------------------')
    if len(blank_lines) > 0:
        for line in blank_lines:
            print("Blank line number: {}".format(line))
    print("\n")
    print("There were {} invalid json objects in the intermediate representation.".format(len(invalid_json)))
    print('---------------------------------------------------------------------------------------')
    if len(invalid_json) > 0:
        print("Lines with invalid json: {}".format(invalid_json))
    print("\n")
    # merge all records into single counter object and print field report
    print('Coverage of fields in IR')
    print('---------------------------------------------------------------------------------------')
    field_report = Counter()
    for record in records:
        for item in record:
            field_report.update({item : 1})
    for item, count in field_report.items():
        print(item + ': ', end ="")
        padding = 80 - len(item)
        for i in range(padding):
            print('-', end ="")
        print(count)

    directory = "/Users/jtim/Dropbox/DLSS/DLME/dlme-harvest/harvest-refactor/analysis-scripts/output"
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open("{}/{}-{}.txt".format(directory, args.function, args.field_one), "w") as f:
        for item in in_clusters:
            f.write("{}: (count: {})\n\n".format(item[0], item[1]))
        f.write("{} of {} records have the {} value".format(field_count, record_count, args.field_one))


# Compare incoming field value to post processing field value
def compare(records):
    in_clusters, out_clusters = cluster_fields(records)
    values = []
    record_has_field_one_count = 0
    record_has_field_two_count = 0
    records_changed = 0
    directory = "/Users/jtim/Dropbox/DLSS/DLME/dlme-harvest/harvest-refactor/analysis-scripts/output"
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open("{}/{}-{}-and-{}.txt".format(directory, args.function, args.field_one, args.field_two), "w") as f:
        for record_count, record in enumerate(records, start=1):
            if args.field_one in record:
                record_has_field_one_count += 1
                field_one = record[args.field_one]
                if args.field_two in record:
                    record_has_field_two_count += 1
                    field_two = record[args.field_two]
                    if field_one == field_two:
                        pass
                    else:
                        records_changed += 1
                        print("{} => {}".format(field_one, field_two))
                        values.append("{} ========> {}".format(field_one, field_two))
        for i in set(values):
            print(i)
            f.write("{}\n\n".format(i))
        if len(in_clusters) == len(out_clusters):
            for in_item, out_item in zip(in_clusters, in_clusters):
                print("{}: {} => {}".format(in_item[1], in_item[0], out_item[0]))
        else:
            print("Warning: the number of incoming values {} does not match the numnber of outgoing values {}.".format(len(in_clusters), len(out_clusters)))
        print("------------------------------------")
        print("Summary:")
        print("{} of {} records have the {} field.".format(record_has_field_one_count, record_count, args.field_one))
        print("{} of {} records have the {} field.".format(record_has_field_two_count, record_count, args.field_two))
        print("{} of {} records transformed.".format(records_changed, record_count))
        f.write("------------------------------------\n")
        f.write("Summary:\n")
        f.write("{} of {} records have the {} field.\n".format(record_has_field_one_count, record_count, args.field_one))
        f.write("{} of {} records have the {} field.\n".format(record_has_field_two_count, record_count, args.field_two))
        f.write("{} of {} records transformed.\n".format(records_changed, record_count))

# Use FIELD_MAP and args.field to determine which validation function to call
def records_missing_field(records):
    for record in records:
        if args.field_one in record:
            pass
        else:
            file = record['dlme_source_file']
            print(file)

def validate_script(records):
    fields = []
    switcher = {
        'ar-Arab': ['a', 'e', 'i', 'o', 'u'],
        'en': ['ا' 'و', 'أ', 'ى', 'ي', 'إ']
    }
    # switcher.get(record[key], "Invalid script")
    for record in records:
        for key, value in record.items():
            if type(value) == dict:
                for key_two, value_two in value.items():
                    if switcher[key_two]:
                        print(switcher[key_two])

                        vowels = record[key]
                        for vowel in vowels:
                            print(vowel)


########## Function Maps ##########

# Function dispatcher to map stage arguments to function names
FUNCTION_MAP = {"inspect": inspect,
                "compare": compare,
                "records_missing_field": records_missing_field,
                "validate_script": validate_script}


########## Main Loop ##########


def main():
    failed_lines = 0
    # Get all ndjson files from output and sort from oldest to newest
    files = sorted(glob.glob("/Users/jtim/Dropbox/DLSS/DLME/dlme-transform/output/*.ndjson"), key=os.path.getmtime)
    func = FUNCTION_MAP[args.function] # Map argument to function dispatcher
    # Get the path to the newest file
    with open(files[-1], "r") as f:
        blank_lines = []
        invalid_json = []
        json_objects = []
        ir = f.readlines()
        for count, line in enumerate(ir, start=1):
            if len(line) == 1:
                blank_lines.append(count)
            else:
                try:
                    json_objects.append(json.loads(line))
                except ValueError as e:
                    invalid_json.append("Line {} does not contain valid json.".format(count))
        # call the funtion passed through as an argument on json_objects
        if args.function == 'inspect':
            func(json_objects, blank_lines, invalid_json)
        else:
            func(json_objects)

if __name__ == "__main__":
    # CLI client options.
    parser = ArgumentParser()
    # Temporarily hard coded file path above
    # parser.add_argument(
    #     "file",
    #     help="What is the path to the ndjson file?")
    parser.add_argument(
        "function", choices=FUNCTION_MAP.keys(),
        help="Which function do you want [inspect, compare, or validate]? ")
    parser.add_argument(
        "field_one",
        nargs="?",
        help="Which field do you want to compare?")
    parser.add_argument(
        "field_two",
        nargs="?",
        help="Which field do you want to compare?")

    args = parser.parse_args()
    main()
