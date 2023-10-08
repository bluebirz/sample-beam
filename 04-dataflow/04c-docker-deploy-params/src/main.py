import apache_beam as beam
from apache_beam.runners.dataflow import DataflowRunner
from CSVToDict import CSVToDictFn
import argparse


def mapToCSVRow(elem: dict) -> str:
    return ",".join(f'"{v}"' for _, v in elem.items())


def run_beam():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", type=str)
    parser.add_argument("--output_file", type=str)
    args, _ = parser.parse_known_args()
    input_file = args.input_file
    output_file = args.output_file
    schema = [
        ("id", int),
        ("first_name", str),
        ("last_name", str),
        ("gender", str),
        ("occupation", str),
    ]
    options = beam.options.pipeline_options.PipelineOptions(
        streaming=False,
        save_main_session=False,
    )
    with beam.Pipeline(
        runner=DataflowRunner(),
        options=options,
    ) as pipe:
        (
            pipe
            | beam.io.ReadFromText(input_file, skip_header_lines=1)
            | beam.ParDo(CSVToDictFn(schema))
            | beam.Filter(lambda l: l["id"] % 2 == 1)
            | beam.Map(mapToCSVRow)
            | beam.io.WriteToText(output_file, shard_name_template="")  # write to file
        )


if __name__ == "__main__":
    run_beam()
