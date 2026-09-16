import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, StandardOptions

def left_join_cycle_stations(element, stations):
    start_station_id = element['start_station_id']
    end_station_id = element['end_station_id']
    
    start_station = stations.get(start_station_id, {})
    end_station = stations.get(end_station_id, {})
    
    start_station_latitude = start_station.get('latitude', None)
    start_station_longitude = start_station.get('longitude', None)
    end_station_latitude = end_station.get('latitude', None)
    end_station_longitude = end_station.get('longitude', None)
    
    return {
        'start_station_id': start_station_id,
        'start_station_name': element['start_station_name'],
        'start_station_latitude': start_station_latitude,
        'start_station_longitude': start_station_longitude,
        'end_station_id': end_station_id,
        'end_station_name': element['end_station_name'],
        'end_station_latitude': end_station_latitude,
        'end_station_longitude': end_station_longitude
    }

# def format_output(element):
#     (start_end_station_id, count_distance) = element
#     start_station_id, end_station_id = start_end_station_id
#     count_distance = int(float(count_distance)) if count_distance is not None else 0
#     return f"{start_station_id},{end_station_id},{count_distance}"

# def format_output(element):
#     (start_end_station_id, count_rides, count_distance) = element
#     start_station_id, end_station_id = start_end_station_id
#     count_distance = int(float(count_distance)) if count_distance is not None else 0
#     return f"{start_station_id},{end_station_id},{count_rides},{count_distance}"

def format_output(element):
    (start_end_station_id, counts) = element
    start_station_id, end_station_id = start_end_station_id
    count_rides = counts['ride_counts'][0] if counts['ride_counts'] else 0
    count_distance = counts['distance_data'][0] if counts['distance_data'] else 0
    count_distance = int(float(count_distance)) if count_distance is not None else 0
    return f"{start_station_id},{end_station_id},{count_rides},{count_distance}"


def run():
    parser = argparse.ArgumentParser(description="Load from BigQuery to GCS")
    parser.add_argument("--project", dest="project", help="Specify Google Cloud project")
    parser.add_argument("--region", dest="region", help="Specify Google Cloud region")
    parser.add_argument("--runner", dest="runner", help="Specify Apache Beam Runner")

    opts, pipeline_opts = parser.parse_known_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_opts)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).job_name = "distance-count-test"
    options.view_as(
        GoogleCloudOptions
    ).service_account_email = 'jain-preetika-career-gmail-com@my-project-practice-420009.iam.gserviceaccount.com'
    options.view_as(StandardOptions).runner = opts.runner

    with beam.Pipeline(options=options) as p:

        from math import radians, sin, cos, sqrt, atan2

        def calculate_distance(lat1, lon1, lat2, lon2):
            # Radius of the Earth in km
            R = 6371.009

            # Convert latitude and longitude from degrees to radians
            if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
                return 0

            lat1 = radians(lat1)
            lon1 = radians(lon1)
            lat2 = radians(lat2)
            lon2 = radians(lon2)

            # Calculate the change in coordinates
            dlon = lon2 - lon1
            dlat = lat2 - lat1

            # Haversine formula to calculate distance
            a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))

            # Calculate the distance
            distance = R * c

            return distance

        cycle_hire_data = (
            p | "Read Cycle Hire Data" >> beam.io.ReadFromBigQuery(
                query='SELECT start_station_id, start_station_name, end_station_id, end_station_name FROM `bigquery-public-data.london_bicycles.cycle_hire`',
                use_standard_sql=True)
        )
    
        cycle_stations_data = (
            p | "Read Cycle Stations Data" >> beam.io.ReadFromBigQuery(
                query='SELECT id, latitude, longitude FROM `bigquery-public-data.london_bicycles.cycle_stations`',
                use_standard_sql=True)
            | "Map Cycle Stations Data" >> beam.Map(lambda row: (row['id'], {'latitude': row['latitude'], 'longitude': row['longitude']}))
        )

        joined_data = (
            cycle_hire_data
            | "Left Join with Cycle Stations" >> beam.Map(
                left_join_cycle_stations,
                stations=beam.pvalue.AsDict(cycle_stations_data)
            )
        )

        # Calculate ride count for each start and end station combination
        ride_counts = (
            joined_data
            | 'Pair start and end stations' >> beam.Map(lambda ride: ((ride['start_station_id'], ride['end_station_id']), 1))
            | 'Count rides for each combination' >> beam.CombinePerKey(sum)
        )

        distance_data = (
            joined_data
            | 'Calculate distance for each ride' >> beam.Map(
                lambda ride: (
                    (ride['start_station_id'], ride['end_station_id']),
                    calculate_distance(
                        ride['start_station_latitude'], ride['start_station_longitude'],
                        ride['end_station_latitude'], ride['end_station_longitude']
                    )
                )
            )
            | 'Tot distance for each combination' >> beam.CombinePerKey(sum)
        )

        ride_count_distance_data = (
                {'ride_counts': ride_counts, 'distance_data': distance_data}
                | 'Merge' >> beam.CoGroupByKey()
            )

        formatted_output_ride_count_distance = ride_count_distance_data | 'Format output Ride Count Distance' >> beam.Map(format_output)

        # Sort the output based on the third column (amount_of_rides) and extract top 200 rows
        sorted_top_200 = (
            formatted_output_ride_count_distance
            | 'Sort and extract top 200 rows' >> beam.combiners.Top.Of(200, key=lambda line: int(line.split(',')[3]))
        )

        sorted_top_200 | 'Write results to output for Ride Count Distance' >> beam.io.WriteToText('gs://practice_challenge/output/ride_distance.txt')

if __name__ == "__main__":
    run()
