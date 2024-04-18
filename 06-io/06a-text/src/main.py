import apache_beam as beam
from apache_beam.runners.dataflow import DataflowRunner
from apache_beam.runners.direct import DirectRunner
import argparse


def run_beam():
    from datetime import datetime

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", type=str)
    parser.add_argument("--output_file", type=str)
    args, _ = parser.parse_known_args()
    options = beam.options.pipeline_options.PipelineOptions()
    with beam.Pipeline(options=options) as pipe:
        (
            pipe
            | "Read" >> beam.io.ReadFromText(args.input_file)
            | "Add time" >> beam.Map(lambda line: f"[{datetime.now()}] {line}")
            | "Write" >> beam.io.WriteToText(args.output_file, shard_name_template="")
        )


if __name__ == "__main__":
    run_beam()
