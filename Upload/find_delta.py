#!/usr/bin/env python
#
# Optional: use unix unique command to drop identical lines first
#
# Open both files
# Read all of first file into a hash table, split off first key
# For each line of second file,
#   If in hash:
#         value matches: remove key from hash
#         value differs: remove key from hash, add both lines to "update"
#   If not in hash:
#         add to "insert"
# Any lines left in hash are "delete"
#
# Right now, we will just send all insert and update lines, unmodified, to the server.
# Delete lines: change all values to null and send that row up.
# Null values in CSV are expressed as the string __null__.
#
# If bandwidth was more sensitive, it would be possible to only send
# up changed values, but the server will do this anyway as part of the
# update transaction.

import logging
import sys

def convert_to_null(input):
    for key in input:
        if input[key] is None:
            input[key] = "__null__"

def output_delta_file(file1_name, file2_name, output, config=None):
    # Quickly get header in a temporary file.
    original_header = open(file1_name).readline().strip().split(',')[1:]
    file1 = open(file1_name)
    file2 = open(file2_name)
    
    (key_column, delete, update, insert) = produce_delta(file1, file2, config)
    first_update_row = update.values()[:1]
    if len(first_update_row) > 0:
        first_update_row = [first_update_row[0][1]]
    first_rows = delete.values()[:1] + first_update_row + insert.values()[:1]
    new_header = list(set(sum(map(dict.keys, first_rows), [])))
    extra_header = list(set(new_header).difference(original_header))
    header = original_header + extra_header
    # If the delta is empty, we won't have a new header, so skip these
    # checks.
    if len(delete) + len(update) + len(insert) > 0:
        assert set(header) == set(new_header)
        assert len(header) == len(new_header)

    print >> output,  ",".join([key_column] + header)
    for key in insert:
        values = insert[key]
        convert_to_null(values)
        print >> output, ','.join([key] + map(values.get, header))
    for key in update:
        values = update[key][1]
        convert_to_null(values)
        print >> output, ','.join([key] + map(values.get, header))
    columns = len(header)
    null_suffix = ""
    for i in xrange(0, columns):
        null_suffix += ",__null__"
    for key in delete:
        print >> output, key + null_suffix

def produce_delta(file1, file2, config=None):
    file1_lines = {}
    file2_lines = {}
    file1_header = file1.readline().strip()
    file2_header = file2.readline().strip()
    file1_columns = file1_header.split(',')
    file1_columns_rest = file1_columns[1:]
    file2_columns = file2_header.split(',')
    file2_columns_rest = file2_columns[1:]

    deleted_columns = list(set(file1_columns).difference(set(file2_columns)))
    deleted_column_map = dict(zip(deleted_columns, [None] * len(deleted_columns)))
    added_columns = list(set(file2_columns).difference(set(file1_columns)))
    added_column_map = dict(zip(added_columns, [None] * len(added_columns)))
    output_len = len(set(file1_columns).union(file2_columns))
    
    if len(file1_columns) < len(file2_columns):
        print "New column introduced. All lines will be found to be a delta."
    elif len(file1_columns) > len(file2_columns):
        print "Column removed. All lines will be found to be a delta."
    if set(file1_columns) != set(file2_columns):
        print "Set of columns changed between files. Added: " \
            + str(added_columns) + ", Removed: " + str(deleted_columns)
    assert file1_columns[0] == file2_columns[0], \
        "First column is interpreted as primary key, and cannot change."
    key_column = file1_columns[0]

    for line in file1:
        # Note: Not safe for escaped comma lines.
        (key, rest) = line.strip().split(',', 1)
        assert len(rest.split(',')) == len(file1_columns) - 1, \
            "Could not split CSV row property: row contains an extra comma: " + line.strip()
        rest_values = dict(zip(file1_columns_rest, rest.split(',')))
        rest_values.update(added_column_map)
        assert len(rest_values) + 1 == output_len
        if key in file1_lines:
            if file1_lines[key] == rest_values:
                logging.error("ERROR: Duplicate line was found in existing input file. "
                              "Because duplicate lines match, this is being ignored. "
                              "However, this is a sign of a serious problem in your "
                              "database or SQL commands. Key: %s File: %s", key, file1)
            else:
                if config and config['options'].discard_duplicate_existing_rows:
                    logging.error("ERROR: Duplicate differing input lines were found in "
                                  "existing input file. Due to --discard_duplicate_input_rows, "
                                  "this will be ignored. Key: %s File: %s", key, file1)
                else:
                    assert not key in file1_lines, \
                        "Duplicate pre-existing key found: " + key + " in file: " + str(file1)
        file1_lines[key] = rest_values

    delete = {}
    update = {}
    insert = {}

    for line in file2:
        # Note: Not safe for escaped comma lines.
        (key, rest) = line.strip().split(',', 1)
        assert len(rest.split(',')) == len(file2_columns) - 1, \
            "Could not split CSV row property: row contains an extra comma: " + line.strip()
        rest_values = dict(zip(file2_columns_rest, rest.split(',')))
        rest_values.update(deleted_column_map)
        assert len(rest_values) + 1 == output_len
        if key in file2_lines:
            if file2_lines[key] == rest_values:
                logging.error("ERROR: Duplicate line was found in new input file. "
                              "Because duplicate lines match, this is being ignored. "
                              "However, this is a sign of a serious problem in your "
                              "database or SQL commands. Key: %s File: %s", key, file2)
            else:
                assert not key in file2_lines, \
                    "Duplicate new key found: " + key + " in file: " + str(file2)
        file2_lines[key] = rest_values

        if not key in file1_lines:
            assert not key in insert, \
                "Duplicate insertion key found: " + key + " in file: " + str(file2)
            insert[key] = rest_values
        else:
            if file1_lines[key] == rest_values:
                pass
            else:
                assert not key in update, \
                    "Duplicate update key found: " + key + " in file: " + str(file2)
                update[key] = (file1_lines[key], rest_values)
            del file1_lines[key]
    
    delete = file1_lines

    return (key_column, delete, update, insert)

def print_duplicates(file1):
    file1_lines = {}
    file1_header = file1.readline().strip()

    count = 0
    for line in file1:
        # Note: Not safe for escaped comma lines.
        (key, rest) = line.strip().split(',', 1)
        if key in file1_lines:
            file1_lines[key] += [rest]
            count += 1
        else:
            file1_lines[key] = [rest]
    print "Duplicate lines: " + str(count)
    for key in file1_lines:
        rows = file1_lines[key]
        if len(rows) > 1:
            for row in rows:
                print ",".join([key, row])

if __name__ == '__main__':
  output_delta_file(sys.argv[1], sys.argv[2], sys.stdout)
