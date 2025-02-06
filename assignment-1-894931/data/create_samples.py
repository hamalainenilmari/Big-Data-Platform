"""
Python script for creating multiple different sample data sets from the original large source data file
for simulating ingesting with multiple writers at the same time 
"""
import csv

if __name__ == '__main__':
    with open("Taxi_Trips__2024-__20250204.csv", "r", newline="") as f: # edit here to match the source data set name of yours
        reader = csv.reader(f)
        header = next(reader)

        fileCount = 1
        while fileCount < 51: # edit this modify number of sample sets created
            chunk = [header]
            for _ in range(20000): # edit this to control how many rows per one input data file
                chunk.append(next(reader))

            currentFile = f"sample{fileCount}.csv"
            with open(currentFile, "w", newline="") as out_f:
                writer = csv.writer(out_f)
                writer.writerows(chunk)
            
            fileCount += 1
            
