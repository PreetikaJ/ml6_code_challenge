import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.runners import DataflowRunner, DirectRunner

def format_output_rides(rides):
    (start_end_station_id, amount_of_rides) = rides
    start_station_id, end_station_id = start_end_station_id
    if start_station_id is not None and end_station_id is not None and amount_of_rides is not None:
        return f"{start_station_id},{end_station_id},{amount_of_rides}"
    else:
        return None

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
    options.view_as(GoogleCloudOptions).job_name = "practice-challenge-easy" #"ml-code-challenge-easy"
    options.view_as(
        GoogleCloudOptions
    ).service_account_email = 'jain-preetika-career-gmail-com@my-project-practice-420009.iam.gserviceaccount.com'
    options.view_as(StandardOptions).runner = opts.runner

    # Beam Pipeline
    with beam.Pipeline(options=options) as p:        
    # Read data from BigQuery
        cycle_hire_data = (
        p | "Read Cycle Hire Data" >> beam.io.ReadFromBigQuery(query='SELECT start_station_id, end_station_id FROM `bigquery-public-data.london_bicycles.cycle_hire`', use_standard_sql=True)
        )

    # Calculate ride count for each start and end station combination
        ride_counts = (
            cycle_hire_data
            | 'Combine start and end stations' >> beam.Map(lambda ride: ((ride['start_station_id'], ride['end_station_id']), 1))
            | 'Count rides for each combination' >> beam.CombinePerKey(sum)
        )
    
    # Format the output
        formatted_output_rides = (
            ride_counts
            | 'Filter None values' >> beam.Filter(lambda rides: rides[0][0] is not None and rides[0][1] is not None and rides[1] is not None)
            | 'Format output data for Rides' >> beam.Map(format_output_rides)
            #| 'Filter None results' >> beam.Filter(lambda output: output is not None)
        )

    # Write the output to a file
        formatted_output_rides | 'Write results to output for ride count' >> beam.io.WriteToText('gs://practice_challenge/output/count_rides')

    p.run()

if __name__ == "__main__":
    run()

# Command to run the file 
# python  ml_code_challenge_easy.py     --project     my-project-practice-420009     --region europe-west10     --runner DataflowRunner

# python -m \
#     ride_count \
#     --project \
#     my-project-practice-420009 \
#     --region eu \
#     --runner DataflowRunner \
#     --outputPath gs://ml_code_challenge/output \
#     --temp_location \
#     gs://ml_code_challenge/temp/

