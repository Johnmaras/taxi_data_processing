### Data files:
- **sub_set.csv**: s3://taxi-data-processing-data/sub_set.csv
- **sub_set_300.csv**: s3://taxi-data-processing-data/sub_set_300.csv
- **fares.csv**: s3://taxi-data-processing-data/fares.csv

Data fields
● id - a unique identifier for each trip
● vendor_id - a code indicating the provider associated with the trip record
● pickup_datetime - date and time when the meter was engaged
● dropoff_datetime - date and time when the meter was disengaged
● passenger_count - the number of passengers in the vehicle (driver entered value)
● pickup_longitude - the longitude where the meter was engaged
● pickup_latitude - the latitude where the meter was engaged
● dropoff_longitude - the longitude where the meter was disengaged
● dropoff_latitude - the latitude where the meter was disengaged
● store_and_fwd_flag - This flag indicates whether the trip record was held in vehicle
memory before sending to the vendor because the vehicle did not have a connection
to the server - Y=store and forward; N=not a store and forward trip
● trip_duration - duration of the trip in seconds

### Queries
1. Split the map into quadrants. How many routes started from each quadrant
2. Count routes R where: **l<sub>R</sub> > 1000m and t<sub>R</sub> > 10mins and p<sub>R</sub> > 2**
3. Find the quadrant of the biggest route
4. Longest route
5. Num of passengers for each vendor
