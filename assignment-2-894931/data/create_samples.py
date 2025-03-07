"""
Python script for creating multiple different sample data sets from the original large source data file
for simulating ingesting with multiple writers at the same time 
"""
import csv
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--file_count", help="number of sample files created")
parser.add_argument("--row_count", help="number of rows in a sample")


args = parser.parse_args()
if __name__ == '__main__':
    with open("../assignment-1-894931/data/Taxi_Trips__2024-__20250204.csv", "r", newline="") as f: # edit here to match the source data set name of yours
        reader = csv.reader(f)
        header = next(reader)

        fileCount = 0
        while fileCount < int(args.file_count): 
            chunk = [header]
            for _ in range(int(args.row_count)):
                chunk.append(next(reader))

            currentFile = f"sample{fileCount}.csv"
            with open(currentFile, "w", newline="") as out_f:
                writer = csv.writer(out_f)
                writer.writerows(chunk)
            
            fileCount += 1
            