import apache_beam as beam

with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Input numbers" >> beam.Create([1, 5, 10, 20, 3])
        | "Filter > 5" >> beam.Filter(lambda x: x > 5)
        | "Show" >> beam.Map(print)
    )
