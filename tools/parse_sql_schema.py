#!/usr/bin/python3

"""NOTE: Currently configured to parse:
https://github.com/cartershanklin/hive-testbench/blob/master/ddl-tpcds/text/alltables.sql

TODO: make sure output files are safe to write (don't contain slashes, etc)
"""

import argparse
import re
import sys

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="SQL file defining the schema")
parser.add_argument("outputdir", help="Output directory to store the schemas")

args = parser.parse_args()

schema_sql = ""
with open(args.filename, "r") as f:
    schema_sql = f.read()

if not schema_sql:
    print("Error reading file")
    sys.exit(1)

raw_schemas = re.findall("create external table.*?\)", schema_sql, re.DOTALL)

table_name_pattern = re.compile("(?<=table)(.*?)(?=\()", re.DOTALL)
col_def_pattern = re.compile("(?<=\().*?(?=\))", re.DOTALL)

for table_schema in raw_schemas:
    # write schema to file
    table_name = table_name_pattern.findall(table_schema)[0].strip()
    filename = args.outputdir + "/" + table_name + ".schema"
    with open(filename, "w") as f:
        # write column as CSV (column, data type)
        col_defs = col_def_pattern.findall(table_schema)[0].split(",")
        lines_to_write = []
        for col_def in col_defs:
            col, col_type = col_def.split()
            lines_to_write.append(col + "," + col_type + "\n")
        f.writelines(lines_to_write)

