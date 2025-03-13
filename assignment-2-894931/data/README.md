# Information about the source data

The data used while simulating using this platform as a tenant is Chicago Taxi Trip data from 2024.

The dataset for testing this platform can be downloaded from: https://data.cityofchicago.org/Transportation/Taxi-Trips-2024-/ajtu-isnz/about_data

Download the set in CSV-format.

The dataset contains: 6.48M rows and 23 columns

sampleData.csv contains 10 rows of the dataset

After downloading the chicago taxi trip data, Create_samples.py (run python3 create_samples.py) can be used for creating sample data sets from the original big data set for
simulating using multiple concurrent producers. Modify the create_samples.py according to match source data set name and how many and how big samples you want to create.

For running stream ingestion with two different tenants, get ny taxi set from: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Download 2024 january Yellow Taxi Trip Records (PARQUET)

### Make sure to check that the taxi trip data set name you downloaded matches the one in the create_samples.py!s