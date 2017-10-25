#!/usr/bin/python3

import numpy as np

# Use as standalone script or import into another script
if __name__ != "__main__":
    from . import table_io


def split_table_into_chunks(table, num_chunks):
    return np.array_split(table, num_chunks)


if __name__ == "__main__":
    import argparse
    import table_io

    parser = argparse.ArgumentParser()
    parser.add_argument("table_name")
    parser.add_argument("num_chunks")
    
    # TODO: add optional arguments for table directory
    # TODO: consider deleting original table

    args = parser.parse_args()

    table_name = args.table_name
    num_chunks = int(args.num_chunks)

    table = table_io.read_table(table_name)
    chunks = split_table_into_chunks(table, num_chunks)

    for i, chunk in enumerate(chunks):
        table_io.write_table_chunk(table_name, chunk, i + 1, num_chunks)

