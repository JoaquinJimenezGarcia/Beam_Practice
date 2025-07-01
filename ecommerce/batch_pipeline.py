import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import csv
import json
from datetime import datetime
import os

class ParseCSV(beam.DoFn):
    def process(self, line):
        try:
            fields = list(csv.reader([line]))[0]
            if fields[0] == "order_id":
                return  # skip header
            return [{
                'order_id': fields[0],
                'user_id': fields[1],
                'timestamp': fields[2],
                'country': fields[3],
                'category': fields[4],
                'quantity': int(fields[7]),
                'amount': float(fields[8])
            }]
        except Exception as e:
            return []

class FormatBQRow(beam.DoFn):
    def process(self, element):
        country, category = element[0]
        quantity = element[1]
        amount = element[2]
        total_amount = quantity*amount
        return [{
            'country': country,
            'category': category,
            'total_sales': total_amount,
            'processed_at': datetime.utcnow().isoformat()
        }]
    
project_id = os.environ["GCP_PROJECT_ID"]
bucketName = os.environ["GCP_BUCKET_NAME"]

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.region = 'europe-west1'
google_cloud_options.job_name = 'sales-batch-job'
google_cloud_options.staging_location = f'gs://{bucketName}/staging'
google_cloud_options.temp_location = f'gs://{bucketName}/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

with beam.Pipeline(options=options) as p:
    (p
     | 'Read CSV' >> beam.io.ReadFromText(f'gs://{bucketName}/sample_sales.csv')
     | 'Parse CSV' >> beam.ParDo(ParseCSV())
     | 'Key by Country and Category' >> beam.Map(lambda x: ((x['country'], x['category']), x['quantity'], x['amount']))
     | 'Format Rows' >> beam.ParDo(FormatBQRow())
     | 'Write to BQ' >> beam.io.WriteToBigQuery(
            f'{project_id}:ecommerce.sales_summary',
            schema={
                'fields': [
                    {'name': 'country', 'type': 'STRING', 'mode': 'REQUIRED'},
                    {'name': 'category', 'type': 'STRING', 'mode': 'REQUIRED'},
                    {'name': 'total_sales', 'type': 'FLOAT', 'mode': 'REQUIRED'},
                    {'name': 'processed_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
                ]
            },
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
    )
