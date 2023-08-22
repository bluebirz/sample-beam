import apache_beam as beam
from apache_beam.runners.direct.direct_runner import DirectRunner

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
    return ",".join(f'"{v}"' for _, v in elem.items())


def run_beam_local():
    options = beam.options.pipeline_options.PipelineOptions(
        save_main_session=True, streaming=False
    )
    with beam.Pipeline(runner=DirectRunner(), options=options) as pipe:
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
    run_beam_local()
