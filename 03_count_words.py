import apache_beam as beam

lines = [
    "Beam makes pipelines easy",
    "Beam is powerful",
    "Apache Beam works with Dataflow"
]

def split_words(line):
    return line.split()

with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Read lines" >> beam.Create(lines)
        | "Split words" >> beam.FlatMap(split_words)
        | "Pair with 1" >> beam.Map(lambda word: (word, 1))
        | "Count words" >> beam.CombinePerKey(sum)
        | "Print results" >> beam.Map(print)
    )
