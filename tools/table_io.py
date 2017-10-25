from collections import OrderedDict
import pandas as pd
import re

# TODO: parse column data types

# Get schema from https://github.com/cartershanklin/hive-testbench/blob/master/ddl-tpcds/text/alltables.sql
DATA_DIR = "tables/"
SCHEMA_DIR = "schema/"

def read_table_schema(table_name):
    """Returns a mapping from column name -> data type
    """
    filename = SCHEMA_DIR + "/" + table_name + ".schema"
    col_map = {}
    with open(filename, "r") as f:
        # Create mapping
        col_map = OrderedDict()
        for line in f.readlines():
            col, col_type = map(str.strip, line.split(","))
            col_map[col] = col_type
            
        return col_map
    
    raise Exception("Couldn't read schema for {table_name}".format(table_name=table_name))

    
def read_table(table_name):
    schema = read_table_schema(table_name)
    filename = DATA_DIR + table_name + ".dat"
    table = pd.read_csv(filename, names=schema.keys(), delimiter='|', index_col=False)
    table.tail()
    return table


def read_table_chunk(table_name, chunk_id, total_chunks):
    header = read_header(table_name)
    filename = "{data_dir}/{table_name}_{chunk_id}_{total_chunks}.dat".format(
        data_dir=DATA_DIR, table_name=table_name, chunk_id=chunk_id, total_chunks=total_chunks)
    table = pd.read_csv(filename, names=header, delimiter='|', index_col=False)
    table.tail()
    return table


def read_table_chunk(table_name, chunk_id, total_chunks):
    schema = read_table_schema(table_name)
    filename = "{data_dir}/{table_name}_{chunk_id}_{total_chunks}.dat".format(
        data_dir=DATA_DIR, table_name=table_name, chunk_id=chunk_id, total_chunks=total_chunks)
    table = pd.read_csv(filename, names=schema.keys(), delimiter='|', index_col=False)
    table.tail()
    return table


def read_bucket(table_name, chunk_id, total_chunks, column, bucket_id, total_buckets):
    schema = read_table_schema(table_name)
    filename_template = "{data_dir}/{name}_{chunk_id}_{total_chunks}_{column}_{bucket_id}_{total_buckets}.dat"
    filename = filename_template.format(data_dir=DATA_DIR, name=table_name, chunk_id=chunk_id,
                                        total_chunks=total_chunks, column=column,
                                        bucket_id=bucket_id, total_buckets=total_buckets)
    table = pd.read_csv(filename, names=schema.keys(), delimiter='|', index_col=False)
    table.tail()
    return table


def write_table_schema(table_name, table):
    filename = SCHEMA_DIR + "/" + table_name + ".schema"
    with open(filename, "w") as f:
        lines_to_write = []
        for col in table.columns:
            # TODO: set column type
            col_type = ""
            lines_to_write.append(col + "," + col_type + "\n")
        f.writelines(lines_to_write)


def write_table(table_name, table):
    write_table_schema(table_name, table)
    filename = "{data_dir}/{table_name}.dat".format(data_dir=DATA_DIR, table_name=table_name)
    table.to_csv(filename, sep='|', header=None)
    
def write_table_chunk(table_name, table, chunk_id, total_chunks):
    write_table_schema(table_name, table)
    filename = "{data_dir}/{table_name}_{chunk_id}_{total_chunks}.dat".format(
        data_dir=DATA_DIR, table_name=table_name, chunk_id=chunk_id, total_chunks=total_chunks)
    table.to_csv(filename, sep='|', header=None)
    
def write_buckets(buckets, table_name, chunk_id, total_chunks, column):
    total_buckets = len(buckets)
    assert total_buckets > 0, "There must be at least 1 bucket"
    write_table_schema(table_name, buckets[0])
    filename_template = "{data_dir}/{name}_{chunk_id}_{total_chunks}_{column}_{bucket_id}_{total_buckets}.dat"
    for i, bucket in enumerate(buckets):
        filename = filename_template.format(data_dir=DATA_DIR, name=table_name,
                                            chunk_id=chunk_id, column=column,
                                            total_chunks=total_chunks, bucket_id=i + 1,
                                            total_buckets=total_buckets)
        bucket.to_csv(filename, sep='|', header=None)
