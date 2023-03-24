import argparse
import sys
import numpy as np
import h5py
import csv
import os
import requests
import tarfile

class ColType:
    UNKNOWN = 1
    STRING = 2
    FLOAT = 3
    INT = 4

# trys to infer a type from a string value
def infer_col_type(str_val):
    str_val = str_val.strip()
    try:
        int(str_val)
        return ColType.INT
    except ValueError:
        try:
            float(str_val)
            return ColType.FLOAT
        except ValueError:
            return ColType.STRING

# takes a sequence of infered column types and returns the most specific type
# that can be used to hold any values in the sequence
def most_specific_common_type(col_types):
    if ColType.STRING in col_types:
        return ColType.STRING
    elif ColType.FLOAT in col_types:
        return ColType.FLOAT
    elif ColType.INT in col_types:
        return ColType.INT
    else:
        return ColType.UNKNOWN

# take a string value along with it's column type and convert it to the python
# native representation
def str_to_native(str_val, col_type):
    if col_type == ColType.STRING:
        return str_val
    elif col_type == ColType.FLOAT:
        return float(str_val)
    elif col_type == ColType.INT:
        return int(str_val)
    else:
        raise ValueError

# converts a CSV file into an HDF5 dataset
def csv_to_hdf5(csv_files, hdf_group):
    row_counter = 0
    experiment_name = "/ATLAS_2019_I1736531/d"
    for counter in range(len(csv_files)):
        if counter == 0:
            # the first pass through the CSV file is to infer column types
            csv_file = open(csv_files[counter], 'r')
            snp_anno_reader = csv.reader(csv_file)
            for i in range(9):
                header = snp_anno_reader.__next__()
                #print(header)
                #print(len(header))
            col_count = len(header)
            #print(col_count)
            col_types = [ColType.UNKNOWN for x in range(0, col_count)]
            row_count = 0
            
            for row in snp_anno_reader:
                if len(row) == col_count:
                    col_types = list(map(most_specific_common_type, zip(map(infer_col_type, row), col_types)))
                    row_count += 1
            
            #print(row_count)

            csv_file.close()

            # the second pass through is to fill in the HDF5 structure
            #hdf5_table = hdf_group.create_dataset(table_name, (row_count,), dtype = table_type)
            csv_file = open(csv_files[counter], 'r')
            snp_anno_reader = csv.reader(csv_file)
            for i in range(9):
                header = snp_anno_reader.__next__()

            #for i in header:
                #print(i)
            category_names = [str_to_native(header[i], ColType.STRING) for i in range(0, col_count)]
            
            
            main_dataset = hdf_group.create_dataset("main", (row_count,8), dtype=h5py.special_dtype(vlen=str), maxshape=(None,12))
            
            for row_index in range(0, row_count):
                row = snp_anno_reader.__next__()
                row_val = [str_to_native(row[i], col_types[i]) for i in range(0, col_count)]
                #print(row_val)
                if counter + 1 < 10:
                    experiment_id = experiment_name + "0" + str(counter+1) + "-x01-y01" + "#" + str(row_index)
                else:
                    experiment_id = experiment_name + str(counter+1) + "-x01-y01" + "#" + str(row_index)
                #experiment_id = experiment_id.encode("utf-8")
                main_dataset[row_counter,0] = experiment_id
                main_dataset[row_counter,1] = str(row_val[3])
                main_dataset[row_counter,2] = str(row_val[1])
                main_dataset[row_counter,3] = str(row_val[2])
                main_dataset[row_counter,4] = str(np.sqrt(row_val[4]**2 + row_val[6]**2 + row_val[8]**2))
                main_dataset[row_counter,5] = str(row_val[4])
                main_dataset[row_counter,6] = str(row_val[6])
                main_dataset[row_counter,7] = str(row_val[8])
                row_counter += 1
                #hdf5_table[row_index] = tuple(row_val)
            
            csv_file.close()
        else:
            print(counter)
            csv_file = open(csv_files[counter], 'r')
            snp_anno_reader = csv.reader(csv_file)
            header = snp_anno_reader.__next__()
            while len(header) != 10:
                header = snp_anno_reader.__next__()
            #header = snp_anno_reader.__next__()
            '''
            for i in range(9):
                header = snp_anno_reader.__next__()
                #print(header)
                #print(len(header))
            '''
            col_count = len(header)
            #print(col_count)
            col_types = [ColType.UNKNOWN for x in range(0, col_count)]
            row_count = 0
            
            for row in snp_anno_reader:
                if len(row) == col_count:
                    col_types = list(map(most_specific_common_type, zip(map(infer_col_type, row), col_types)))
                    row_count += 1

            csv_file.close()

            csv_file = open(csv_files[counter], 'r')
            snp_anno_reader = csv.reader(csv_file)
            header = snp_anno_reader.__next__()
            while len(header) != 10:
                header = snp_anno_reader.__next__()
            #header = snp_anno_reader.__next__()
            '''
            for i in range(9):
                header = snp_anno_reader.__next__()
            '''
            main_dataset.resize((main_dataset.shape[0] + row_count), axis=0)
            for row_index in range(0, row_count):
                row = snp_anno_reader.__next__()
                row_val = [str_to_native(row[i], col_types[i]) for i in range(0, col_count)]
                #print(row_val)
                if counter + 1 < 10:
                    experiment_id = experiment_name + "0" + str(counter+1) + "-x01-y01" + "#" + str(row_index)
                else:
                    experiment_id = experiment_name + str(counter+1) + "-x01-y01" + "#" + str(row_index)
                #experiment_id = experiment_id.encode("utf-8")
                main_dataset[row_counter,0] = experiment_id
                main_dataset[row_counter,1] = str(row_val[3])
                main_dataset[row_counter,2] = str(row_val[1])
                main_dataset[row_counter,3] = str(row_val[2])
                main_dataset[row_counter,4] = str(np.sqrt(row_val[4]**2 + row_val[6]**2 + row_val[8]**2))
                main_dataset[row_counter,5] = str(row_val[4])
                main_dataset[row_counter,6] = str(row_val[6])
                main_dataset[row_counter,7] = str(row_val[8])
                row_counter += 1
                #hdf5_table[row_index] = tuple(row_val)
            
            csv_file.close()

            #if os.path.isfile(f):
                #print(f[:-4])
            #hdf5_file = h5py.File(f[:-4] + ".h5", 'w')
            #csv_to_hdf5(f, hdf_group)
        main_dataset.attrs.__setitem__("row_counter", row_counter)
        main_dataset.attrs.__setitem__("format", "index/value/bin_low/bin_high/total uncertainty (quadrature)/statistical uncertainty/detector uncertainty/generator uncertainty")

# main entry point for script
def main():
    parser = argparse.ArgumentParser(description = 'Convert a CSV file to HDF5')
    '''
    parser.add_argument(
        'CSV_input_file',
        help = 'the CSV input file')
    parser.add_argument(
        'HDF5_output_file',
        help = 'the HDF5 output file')
    '''
    parser.add_argument(
        'url',
        help = 'the url from which we are downloading')
    parser.add_argument(
        'filepath',
        help = 'the folder where we are making the h5 files')
    args = parser.parse_args()
    response = requests.get(args.url, stream=True)
    file = tarfile.open(fileobj=response.raw, mode="r|gz")
    file.extractall(path=args.filepath)
    hdf5_file = h5py.File("HEPData-ins1736531.h5", 'w')
    os.chdir(args.filepath + "/" + "HEPData-ins1736531-v1-csv")

    csv_files = os.listdir(os.getcwd())
    csv_files = [os.path.join(os.getcwd(), f) for f in csv_files]
    csv_files.sort(key=lambda f: int(''.join(filter(str.isdigit, f))))
    csv_to_hdf5(csv_files, hdf5_file)

if __name__ == "__main__":
    main()