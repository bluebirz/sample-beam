import apache_beam as beam
from apache_beam.runners.dataflow import DataflowRunner
from apache_beam.runners.direct import DirectRunner
import argparse
from functions.firestore_writer import FireStoreWriterToCollection
from functions.firestore_reader import FireStoreReaderFromCollection


def run_beam():
    parser = argparse.ArgumentParser()
    # parser.add_argument("--input_file", type=str)
    # parser.add_argument("--output_file", type=str)
    # args, _ = parser.parse_known_args()
    # input_file = args.input_file
    # output_file = args.output_file
    with beam.Pipeline(
        runner=DirectRunner(),
        options=beam.options.pipeline_options.PipelineOptions(),
    ) as pipe:
        (
            pipe
            | "Read" >> beam.Create([{"key": "bluebirz", "value": {"name": "BBZ"}}])
            | "Write" >> beam.ParDo(FireStoreWriterToCollection("EU"))
            | "Get" >> beam.ParDo(FireStoreReaderFromCollection("EU"))
            | "Print" >> beam.Map(print)
        )


if __name__ == "__main__":
    run_beam()
