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


def run():
    # runner = RenderRunner()
    runner = "DirectRunner"
    options = beam.options.pipeline_options.PipelineOptions(
        save_main_session=True, streaming=False
    )
    with beam.Pipeline(runner=runner, options=options) as pipe:
        out = (
            pipe
            | beam.io.ReadFromText("assets/mock.csv", skip_header_lines=1)
            | beam.Map(mapToDict)
            | beam.Filter(lambda l: l["gender"] == "F")
        )
        (out | beam.Map(print))
        (
            out
            | beam.Map(mapToCSVRow)
            | beam.io.WriteToText("assets/processed.csv", shard_name_template="")
        )


if __name__ == "__main__":
    run()
