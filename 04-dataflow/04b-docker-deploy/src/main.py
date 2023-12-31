import apache_beam as beam
from apache_beam.runners.dataflow import DataflowRunner
from CSVToDict import CSVToDictFn


def mapToCSVRow(elem: dict) -> str:
    return ",".join(f'"{v}"' for _, v in elem.items())


def run_beam():
    input_file = "gs://bluebirz-beam/dataflow-1/mock.csv"
    output_file = "gs://bluebirz-beam/dataflow-1/processed.csv"
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
            | beam.Filter(lambda l: l["gender"] == "M")
            | beam.Map(mapToCSVRow)
            | beam.io.WriteToText(output_file, shard_name_template="")  # write to file
        )


if __name__ == "__main__":
    run_beam()
