import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

def left_join_cycle_hires_stations(hires, stations):
    start_station_id = hires['start_station_id']
    end_station_id = hires['end_station_id']
    
    start_station = stations.get(start_station_id, {})
    end_station = stations.get(end_station_id, {})
    
    start_station_latitude = start_station.get('latitude', None)
    start_station_longitude = start_station.get('longitude', None)
    end_station_latitude = end_station.get('latitude', None)
    end_station_longitude = end_station.get('longitude', None)
    
    return {
        'start_station_id': start_station_id,
        'start_station_name': hires['start_station_name'],
        'start_station_latitude': start_station_latitude,
        'start_station_longitude': start_station_longitude,
        'end_station_id': end_station_id,
        'end_station_name': hires['end_station_name'],
        'end_station_latitude': end_station_latitude,
        'end_station_longitude': end_station_longitude
    }

def format_output_ride_distance(ride_distance):
    (start_end_station_id, counts) = ride_distance
    start_station_id, end_station_id = start_end_station_id
    if start_station_id is not None and end_station_id is not None:
        count_rides = counts['ride_counts'][0] if counts['ride_counts'] else 0
        count_distance = counts['distance_data'][0] if counts['distance_data'] else 0
        count_distance = int(float(count_distance)) if count_distance is not None else 0
        return f"{start_station_id},{end_station_id},{count_rides},{count_distance}"
    else:
        return "0,0,0,0"

def run():
    """Runs the apache beam pipeline

    Parameters
    ----------
    parse_known_args : argparse namespace object
                 description for parse arguments
                 specified in help
    """

    parser = argparse.ArgumentParser(description="Load output text files to GCS")
    parser.add_argument(
        "--project", dest="project", help="Specify Google Cloud project"
    )
    parser.add_argument("--region", dest="region", help="Specify Google Cloud region")
    parser.add_argument("--runner", dest="runner", help="Specify Apache Beam Runner")
    parser.add_argument("--temp_location", dest="temp_location", help="temp location")

    opts, pipeline_opts = parser.parse_known_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_opts)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).temp_location = 'gs://practice_challenge/temp/'
    options.view_as(GoogleCloudOptions).job_name = "practice-challenge-hard" #"ml-code-challenge-hard"
    options.view_as(
        GoogleCloudOptions
    ).service_account_email = 'jain-preetika-career-gmail-com@my-project-practice-420009.iam.gserviceaccount.com'
    options.view_as(StandardOptions).runner = opts.runner

    # Beam Pipeline
    with beam.Pipeline(options=options) as p:
        # define a function for calculating the distance in km 
        from math import radians, sin, cos, sqrt, atan2

        def calculate_distance(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude):
            # Radius of the Earth in km
            earth_radius = 6371.009

            # Convert latitude and longitude from degrees to radians
            if start_station_latitude is None or start_station_longitude is None or end_station_latitude is None or end_station_longitude is None:
                return 0 # Because None type cannot be sorted

            start_station_latitude = radians(start_station_latitude)
            start_station_longitude = radians(start_station_longitude)
            end_station_latitude = radians(end_station_latitude)
            end_station_longitude = radians(end_station_longitude)

            # Calculate the change in coordinates
            diff_longitude = end_station_longitude - start_station_longitude
            diff_latitude = end_station_latitude - start_station_latitude

            # Haversine formula to calculate distance
            a = sin(diff_latitude / 2)**2 + cos(start_station_latitude) * cos(end_station_latitude) * sin(diff_longitude / 2)**2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))

            # Calculate the distance
            distance = earth_radius * c

            return distance
        
    # Read data from BigQuery
        cycle_hire_data = (
        p | "Read Cycle Hire Data" >> beam.io.ReadFromBigQuery(query='SELECT start_station_id, start_station_name, end_station_id, end_station_name FROM `bigquery-public-data.london_bicycles.cycle_hire`', use_standard_sql=True)
        )
    
        cycle_stations_data = (
        p | "Read Cycle Stations Data" >> beam.io.ReadFromBigQuery(query='SELECT id, latitude, longitude FROM `bigquery-public-data.london_bicycles.cycle_stations`', use_standard_sql=True)
        | "Map Cycle Stations Data" >> beam.Map(lambda row: (row['id'], {'latitude': row['latitude'], 'longitude': row['longitude']}))
        )

    # Perform left join between cycle hire and cycle stations data
        joined_data = (
            cycle_hire_data
            | "Cycle Hires Left Join with Cycle Stations" >> beam.Map(
                left_join_cycle_hires_stations,
                stations=beam.pvalue.AsDict(cycle_stations_data)
            )
        )

    # Calculate ride count for each start and end station combination
        ride_counts = (
            cycle_hire_data
            | 'Combine start and end stations' >> beam.Map(lambda ride: ((ride['start_station_id'], ride['end_station_id']), 1))
            | 'Count rides for each combination' >> beam.CombinePerKey(sum)
        )

    # Calculate total distance for each ride
        distance_data = (
            joined_data
            | 'Calculate distance for each combination of start and end station' >> beam.Map(
                lambda ride: (
                    (ride['start_station_id'], ride['end_station_id']),
                    calculate_distance(
                        ride['start_station_latitude'], ride['start_station_longitude'],
                        ride['end_station_latitude'], ride['end_station_longitude']
                    )
                )
            )
            | 'Total distance for each combination' >> beam.CombinePerKey(sum)
        )   

        ride_count_distance_data = (
                {'ride_counts': ride_counts, 'distance_data': distance_data}
                | 'Merge' >> beam.CoGroupByKey()
            )
        
    # Format the output         
        formatted_output_ride_count_distance = ride_count_distance_data | 'Format output data for Ride Count and Distance' >> beam.Map(format_output_ride_distance)

    # Write the output to a file
        formatted_output_ride_count_distance | 'Write results to output for Ride Count and Distance' >> beam.io.WriteToText('gs://practice_challenge/output/count_ride_distance')

    p.run()


if __name__ == "__main__":
    run()

# Command to run the file 
# python  ml_code_challenge_hard.py     --project     my-project-practice-420009     --region europe-west10     --runner DataflowRunner

# python -m \
#     ride_count \
#     --project \
#     my-project-practice-420009 \
#     --region eu \
#     --runner DataflowRunner \
#     --outputPath gs://ml_code_challenge/output \
#     --temp_location \
#     gs://ml_code_challenge/temp/

