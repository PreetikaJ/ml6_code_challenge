import argparse
from datetime import date
import logging
import apache_beam as beam
import json, csv
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.runners import DataflowRunner, DirectRunner

def run():
    """Runs the apache beam pipeline

    Parameters
    ----------
    parse_known_args : argparse namespace object
                 description for parse arguments
                 specified in help
    """

    parser = argparse.ArgumentParser(description="Load from BigQuery to GCS")
    parser.add_argument(
        "--project", dest="project", help="Specify Google Cloud project"
    )
    parser.add_argument("--region", dest="region", help="Specify Google Cloud region")
    parser.add_argument("--runner", dest="runner", help="Specify Apache Beam Runner")
    parser.add_argument(
        "--outputPath", dest="outputPath", help="Path to coldline storage bucket"
    )
    parser.add_argument("--temp_location", dest="temp_location", help="temp location")

    opts, pipeline_opts = parser.parse_known_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_opts)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = "ride_count"
    options.view_as(StandardOptions).runner = opts.runner

    output_path = opts.outputPath

    query = """
    SELECT
        start_station_name,
        end_station_name
    FROM
        `bigquery-public-data.london_bicycles.cycle_hire`
    """

    # Beam Pipeline
    with beam.Pipeline(options=options) as p:
    # Read data from BigQuery
        rides = (
            p | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=query)
        )
        print(rides)

    # Calculate ride count for each start and end station combination
        ride_counts = (
            rides
            | 'Pair start and end stations' >> beam.Map(lambda ride: ((ride['start_station_name'], ride['end_station_name']), 1))
            | 'Count rides for each combination' >> beam.CombinePerKey(sum)
        )

    # Write the results to a text file
        ride_counts | 'Write results to output' >> beam.io.WriteToText(output_path)

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")
    
    p.run().wait_until_finish()


# python -m count_rides.py --region eu --outputPath gs://dataflow-apache-quickstart_my-project-practice-420009/output --runner DataflowRunner --project my-project-practice-420009 --temp_location gs://dataflow-apache-quickstart_my-project-practice-420009/temp/