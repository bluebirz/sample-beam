import apache_beam as beam
from apache_beam.runners.dataflow import DataflowRunner
import argparse
from datetime import datetime


def run_beam():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", type=str)
    parser.add_argument("--output_file", type=str)
    args, _ = parser.parse_known_args()
    input_file = args.input_file
    output_file = args.output_file
    with beam.Pipeline(
        runner=DataflowRunner(),
        options=beam.options.pipeline_options.PipelineOptions(),
    ) as pipe:
        (
            pipe
            | "Read" >> beam.io.ReadFromText(input_file)
            | "Add time" >> beam.Map(lambda line: f"[{datetime.now()}] {line}")
            | "Write" >> beam.io.WriteToText(output_file, shard_name_template="")
        )


if __name__ == "__main__":
    run_beam()
