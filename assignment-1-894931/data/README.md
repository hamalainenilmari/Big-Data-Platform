The dataset for testing this platform can be downloaded from:
https://data.cityofchicago.org/Transportation/Taxi-Trips-2024-/ajtu-isnz/about_data

Download the set in CSV-format.

The dataset contains: 6.48M rows and 23 columns

sampleData.csv contains 1 rows of the dataset

After downloading the chicago taxi trip data, Create_samples.py can be used for creating sample data sets from the original big data set for
simulating using multiple concurrent producers. Modify the create_samples.py according to match source data set name and how many and how big samples you want to create.

### Make sure to check that the taxi trip data set name you downloaded matches the one in the create_samples.py!