"""
Python script for creating multiple different sample data sets from the original source data 
for simulating ingesting with multiple writers at the same time 
"""
import csv

if __name__ == '__main__':
    with open("taxiTrips.csv", "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)  # header line

        fileCount = 0
        while fileCount < 10:
            chunk = [header]
            for _ in range(10000):
                chunk.append(next(reader))

            currentFile = f"sample{fileCount}.csv"
            with open(currentFile, "w", newline="") as out_f:
                writer = csv.writer(out_f)
                writer.writerows(chunk)
            
            fileCount += 1
            
