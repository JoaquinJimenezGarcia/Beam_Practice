import apache_beam as beam

def to_uppercase(element):
    return element.upper()

with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Read lines" >> beam.Create(["hello", "world", "beam"])
        | "Uppercase" >> beam.Map(to_uppercase)
        | "Print results" >> beam.Map(print)
    )
