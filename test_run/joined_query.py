import apache_beam as beam

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

with beam.Pipeline() as p:
    cycle_hire_data = (
        p | "Read Cycle Hire Data" >> beam.io.ReadFromBigQuery(query='SELECT * FROM `bigquery-public-data.london_bicycles.cycle_hire`')
    )
    
    cycle_stations_data = (
        p | "Read Cycle Stations Data" >> beam.io.ReadFromBigQuery(query='SELECT * FROM `bigquery-public-data.london_bicycles.cycle_stations`')
        | "Map Cycle Stations Data" >> beam.Map(lambda row: (row['id'], row))
    )
    
    joined_data = (
        cycle_hire_data
        | "Left Join with Cycle Stations" >> beam.Map(left_join_cycle_stations, stations=beam.pvalue.AsDict(cycle_stations_data))
    )

    # Write joined_data to BigQuery
    _ = joined_data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
        table='your-project-id.your_dataset_id.your_table_id',
        schema='start_station_id:INTEGER,start_station_name:STRING,start_station_latitude:FLOAT,start_station_longitude:FLOAT,end_station_id:INTEGER,end_station_name:STRING,end_station_latitude:FLOAT,end_station_longitude:FLOAT',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )
