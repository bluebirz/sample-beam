import apache_beam as beam
from apache_beam.runners.render import RenderRunner
import csv


def mapToDict(elem: str) -> dict:
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
    return ",".join(f'"{v}"' for k, v in elem.items())


def run_beam():
    with beam.Pipeline(
        runner=RenderRunner(),
        options=beam.options.pipeline_options.PipelineOptions(),
    ) as pipe:
        (
            pipe
            # add step names in format `"..." >> beam.xxx`
            | "Read CSV" >> beam.io.ReadFromText("assets/mock.csv", skip_header_lines=1)
            | "Map CSV to dict" >> beam.Map(mapToDict)
            | "only Female" >> beam.Filter(lambda l: l["gender"] == "F")
            | "Map dict to CSV" >> beam.Map(mapToCSVRow)
            | "Write CSV"
            >> beam.io.WriteToText("assets/processed.csv", shard_name_template="")
        )


if __name__ == "__main__":
    run_beam()
