import apache_beam as beam
import argparse
from functions.firestore_writer import FireStoreWriterToCollection
from functions.firestore_reader import FireStoreReaderFromCollection


def run_beam():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_collection", type=str)
    parser.add_argument("--destination_collection", type=str)
    args, _ = parser.parse_known_args()
    options = beam.options.pipeline_options.PipelineOptions()
    with beam.Pipeline(options=options) as pipe:
        (
            pipe
            | "Read"
            >> beam.io.Read(FireStoreReaderFromCollection(args.source_collection))
            | "Write"
            >> beam.ParDo(FireStoreWriterToCollection(args.destination_collection))
        )


if __name__ == "__main__":
    run_beam()
