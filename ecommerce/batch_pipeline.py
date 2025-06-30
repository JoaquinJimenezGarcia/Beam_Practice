import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import csv
import json
from datetime import datetime

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
                'amount': float(fields[5])
            }]
        except Exception as e:
            return []

class FormatBQRow(beam.DoFn):
    def process(self, element):
        country, category = element[0]
        total_amount = element[1]
        return [{
            'country': country,
            'category': category,
            'total_sales': total_amount,
            'processed_at': datetime.utcnow().isoformat()
        }]

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'your-gcp-project'
google_cloud_options.region = 'europe-west1'
google_cloud_options.job_name = 'sales-batch-job'
google_cloud_options.staging_location = 'gs://my-bucket/staging'
google_cloud_options.temp_location = 'gs://my-bucket/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

with beam.Pipeline(options=options) as p:
    (p
     | 'Read CSV' >> beam.io.ReadFromText('gs://my-bucket/daily_sales/ventas_2025_06_25.csv')
     | 'Parse CSV' >> beam.ParDo(ParseCSV())
     | 'Key by Country and Category' >> beam.Map(lambda x: ((x['country'], x['category']), x['amount']))
     | 'Sum Sales' >> beam.CombinePerKey(sum)
     | 'Format Rows' >> beam.ParDo(FormatBQRow())
     | 'Write to BQ' >> beam.io.WriteToBigQuery(
            'your-gcp-project:ecommerce.sales_summary',
            schema='country:STRING,category:STRING,total_sales:FLOAT,processed_at:TIMESTAMP',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
