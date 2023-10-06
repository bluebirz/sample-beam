import apache_beam as beam
from apache_beam.runners.dataflow import DataflowRunner


def mapToDict(elem: str) -> dict:
    import csv

    for l in csv.reader(
        [elem],
        delimiter=",",
        quotechar='"',
        quoting=csv.QUOTE_ALL,
        skipinitialspace=True,
    ):
        result = {
            "id": int(l[0]),
            "first_name": l[1],
            "last_name": l[2],
            "gender": l[3],
            "occupation": l[4],
        }
    return result


def mapToCSVRow(elem: dict) -> str:
    return ",".join(f'"{v}"' for _, v in elem.items())


def run_beam():
    input_file = "gs://bluebirz-beam/dataflow-1/mock.csv"
    output_file = "gs://bluebirz-beam/dataflow-1/processed.csv"
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
            | beam.Map(mapToDict)
            | beam.Filter(lambda l: l["gender"] == "F")
            | beam.Map(mapToCSVRow)
            | beam.io.WriteToText(output_file, shard_name_template="")  # write to file
        )


if __name__ == "__main__":
    run_beam()
