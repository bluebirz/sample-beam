import apache_beam as beam
import argparse
from functions.firestore_writer import FireStoreWriterToCollection
from functions.firestore_reader import FireStoreReaderFromCollection


def run_beam():
    parser = argparse.ArgumentParser()
    # parser.add_argument("--input_file", type=str)
    # parser.add_argument("--output_file", type=str)
    # args, _ = parser.parse_known_args()
    options = beam.options.pipeline_options.PipelineOptions()
    with beam.Pipeline(options=options) as pipe:
        (
            pipe
            | "Read" >> beam.Create([{"key": "bluebirz", "value": {"name": "BBZ"}}])
            | "Write" >> beam.ParDo(FireStoreWriterToCollection("EU"))
            | "Get" >> beam.ParDo(FireStoreReaderFromCollection("EU"))
            | "Print" >> beam.Map(print)
        )


if __name__ == "__main__":
    run_beam()
