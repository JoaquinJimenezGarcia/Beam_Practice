import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json
from datetime import datetime
import os

class ParseEvent(beam.DoFn):
    def process(self, element):
        try:
            row = json.loads(element.decode('utf-8'))
            return [{
                'event_type': row['event_type'],
                'timestamp': row['timestamp']
            }]
        except:
            return []

class FormatStats(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        event_type, count = element
        window_start = window.start.to_utc_datetime().isoformat()
        return [{
            'event_type': event_type,
            'event_count': count,
            'window_start': window_start,
            'processed_at': datetime.utcnow().isoformat()
        }]
    
project_id = os.environ["GCP_PROJECT_ID"]
bucketName = os.environ["GCP_BUCKET_NAME"]

options = PipelineOptions(streaming=True)
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.region = 'europe-west1'
google_cloud_options.job_name = 'user-event-stream'
google_cloud_options.staging_location = f'gs://{bucketName}/staging'
google_cloud_options.temp_location = f'gs://{bucketName}/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

with beam.Pipeline(options=options) as p:
    (p
     | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic=f'projects/{project_id}/topics/user-events')
     | 'Parse Event' >> beam.ParDo(ParseEvent())
     | 'Key by Event Type' >> beam.Map(lambda x: (x['event_type'], 1))
     | 'Window into 1 minute' >> beam.WindowInto(beam.window.FixedWindows(60))
     | 'Count Events' >> beam.CombinePerKey(sum)
     | 'Format Stats' >> beam.ParDo(FormatStats())
     | 'Write to BQ' >> beam.io.WriteToBigQuery(
            f'{project_id}:ecommerce.user_events_stats',
            schema='event_type:STRING,event_count:INTEGER,window_start:TIMESTAMP,processed_at:TIMESTAMP',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
