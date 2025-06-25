import apache_beam as beam
import apache_beam.io.gcp.gcsio
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import csv

class ParseCSV(beam.DoFn):
    def process(self, element):
        # Parse CSV row
        values = list(csv.reader([element]))[0]
        if values[0] == "date":
            return []  # skip header
        return [{
            'date': values[0],
            'product': values[1],
            'quantity': int(values[2]),
            'price': float(values[3])
        }]

def run():
    project_id = 'jjgdevelopment-399314'
    bucketName = 'bucket_test_cloudarch'
    file = f'gs://{bucketName}/products.csv'
    bq_table = 'jjgdevelopment-399314:ventas_dataset.transacciones'

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.region = 'europe-west1'
    google_cloud_options.job_name = 'dataflow-csv-bq-job'
    google_cloud_options.staging_location = f'gs://{bucketName}/staging'
    google_cloud_options.temp_location = f'gs://{bucketName}/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # DEFINICIÓN PIPELINE
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read CSV from GCS' >> beam.io.ReadFromText(file)
            | 'Parse CSV rows' >> beam.ParDo(ParseCSV())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                bq_table,
                schema='fecha:DATE,producto:STRING,cantidad:INTEGER,precio:FLOAT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
