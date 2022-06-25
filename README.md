# pyspark-bigdata


Problems 1 and 2 are based on dataset eBooks.zip



PROBLEM 1
Find frequent words (whose count is equal or greater than 49,500).
Display the frequent words in descending order.

PROBLEM 2
Find groups of words by their length and count each group.
For example, (2,1096049) means that there are 1096049 occurrence of words that have two
characters.
Find frequent groups (count in the group is equal or greater than 495,000).
Display the frequent groups in descending order.


Problem 3 is based on dataset nyc_taxi_data_2014.csv.gz
PROBLEM 3
(a) Create a RDD variable ‘nyc2’ that has passenger_count, trip_distance, and fare_amount only
with the records of passenger_count 1 through 9.
(b) Display the passenger_count, record counts, average trip_distance, and average fare_amount
for each group of passenger_count. Order them by passenger_account in ascending order
