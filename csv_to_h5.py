import argparse
import numpy as np
import h5py
import csv
import os
import requests
import tarfile

# converts a CSV file into an HDF5 dataset
def csv_to_hdf5(csv_files, hdf_group, rivet_ID, csv_col_num):
    row_counter = hdf_group["main"].attrs.__getitem__("row_counter")
    experiment_name = "/" + rivet_ID + "/d"
    row_len = int(csv_col_num)
    for counter in range(len(csv_files)):
        # the first pass through the CSV file is to count the number of rows to reshape the h5 file by
        print(counter)
        csv_file = open(csv_files[counter], 'r')
        snp_anno_reader = csv.reader(csv_file)
        header = snp_anno_reader.__next__()
        while len(header) != row_len:
            header = snp_anno_reader.__next__()
        row_count = 0
        
        while len(header) == row_len:
            header = snp_anno_reader.__next__()
            if len(header) == row_len:
                row_count += 1

        for row in snp_anno_reader:
            header = row
            while len(header) != row_len:
                header = snp_anno_reader.__next__()

            while len(header) == row_len:
                header = snp_anno_reader.__next__()
                if len(header) == row_len:
                    row_count += 1

        csv_file.close()

        # the second pass through is to fill in the HDF5 structure
        csv_file = open(csv_files[counter], 'r')
        snp_anno_reader = csv.reader(csv_file)
        header = snp_anno_reader.__next__()
        while len(header) != row_len:
            header = snp_anno_reader.__next__()
        
        main_dataset = hdf_group["main"]
        
        main_dataset.resize((main_dataset.shape[0] + row_count), axis=0)

        row_index = 0

        while len(header) == row_len:
            header = snp_anno_reader.__next__()
            if len(header) == row_len:
                row_val = header
                if counter + 1 < 10:
                    experiment_id = experiment_name + "0" + str(counter+1) + "-x01-y01" + "#" + str(row_index)
                else:
                    experiment_id = experiment_name + str(counter+1) + "-x01-y01" + "#" + str(row_index)
                main_dataset[row_counter,0] = experiment_id
                if str(row_val[3]) == "-":
                        main_dataset[row_counter,1] = "9999"
                else:
                        main_dataset[row_counter,1] = str(row_val[3])
                main_dataset[row_counter,2] = str(row_val[1])
                main_dataset[row_counter,3] = str(row_val[2])
                '''
                main_dataset[row_counter,4] = str(np.sqrt(float(row_val[4])**2 + float(row_val[6])**2))
                main_dataset[row_counter,5] = str(row_val[4])
                main_dataset[row_counter,6] = str(row_val[6])
                
                '''
                main_dataset[row_counter,4] = str(row_val[4])
                main_dataset[row_counter,5] = "0"
                main_dataset[row_counter,6] = "0"
                

                main_dataset[row_counter,7] = "0"
                #main_dataset[row_counter,4] = str(np.sqrt(row_val[4]**2 + row_val[6]**2 + row_val[8]**2))
                #main_dataset[row_counter,5] = str(row_val[4])
                #main_dataset[row_counter,6] = str(row_val[6])
                #main_dataset[row_counter,7] = str(row_val[8])
                row_counter += 1
                row_index += 1         

        y_counter = 2

        for row in snp_anno_reader:
            row_index = 0
            header = row
            while len(header) != row_len:
                header = snp_anno_reader.__next__()

            while len(header) == row_len:
                header = snp_anno_reader.__next__()
                if len(header) == row_len:
                    row_val = header
                    if counter + 1 < 10:
                        experiment_id = experiment_name + "0" + str(counter+1) + "-x01-y0" + str(y_counter) + "#" + str(row_index)
                    else:
                        experiment_id = experiment_name + str(counter+1) + "-x01-y0" + str(y_counter) + "#" + str(row_index)
                    main_dataset[row_counter,0] = experiment_id
                    if str(row_val[3]) == "-":
                        main_dataset[row_counter,1] = "9999"
                    else:
                        main_dataset[row_counter,1] = str(row_val[3])
                    main_dataset[row_counter,2] = str(row_val[1])
                    main_dataset[row_counter,3] = str(row_val[2])

                    '''
                    main_dataset[row_counter,4] = str(np.sqrt(float(row_val[4])**2 + float(row_val[6])**2))
                    main_dataset[row_counter,5] = str(row_val[4])
                    main_dataset[row_counter,6] = str(row_val[6])

                    '''
                    main_dataset[row_counter,4] = str(row_val[4])
                    main_dataset[row_counter,5] = "0"
                    main_dataset[row_counter,6] = "0"
                    

                    main_dataset[row_counter,7] = "0"
                    #main_dataset[row_counter,4] = str(np.sqrt(row_val[4]**2 + row_val[6]**2 + row_val[8]**2))
                    #main_dataset[row_counter,5] = str(row_val[4])
                    #main_dataset[row_counter,6] = str(row_val[6])
                    #main_dataset[row_counter,7] = str(row_val[8])
                    row_counter += 1
                    row_index += 1
            
            y_counter += 1
        
        csv_file.close()
        
    main_dataset.attrs.__setitem__("row_counter", row_counter)

# main entry point for script
def main():
    parser = argparse.ArgumentParser(description = 'Convert a CSV file to HDF5')
    parser.add_argument(
        'url',
        help = 'the url from which we are downloading')
    parser.add_argument(
        'filepath',
        help = 'the folder where we are making the h5 files')
    parser.add_argument(
        'h5_filename',
        help = 'h5 filename')
    parser.add_argument(
        'csv_dir',
        help = 'the folder with the CSVs')
    parser.add_argument(
        'rivet_ID',
        help = 'experiment rivet ID')
    parser.add_argument(
        'csv_col_num',
        help = 'numbers of columns in CSV')
    '''
    parser.add_argument(
        'uncertainty_breakdown',
        help = 'boolean representing whether the total uncertainty has been broken down into components'
    )
    '''
    args = parser.parse_args()
    response = requests.get(args.url, stream=True)
    file = tarfile.open(fileobj=response.raw, mode="r|gz")
    file.extractall(path=args.filepath)
    hdf5_file = h5py.File(args.h5_filename, 'r+')
    os.chdir(args.filepath + "/" + args.csv_dir)
    
    csv_files = os.listdir(os.getcwd())
    csv_files = [os.path.join(os.getcwd(), f) for f in csv_files]
    csv_files.sort(key=lambda f: int(''.join(filter(str.isdigit, f))))
    csv_to_hdf5(csv_files, hdf5_file, args.rivet_ID, args.csv_col_num)

if __name__ == "__main__":
    main()