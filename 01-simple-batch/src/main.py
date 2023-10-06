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
    return ",".join(f'"{v}"' for k, v in elem.items())


def run_beam():
    with beam.Pipeline(runner=DirectRunner()) as pipe:
        (
            pipe
            | beam.io.ReadFromText("assets/mock.csv", skip_header_lines=1)
            | beam.Map(mapToDict)
            | beam.Filter(lambda l: l["gender"] == "F")
            | beam.Map(mapToCSVRow)
            | beam.io.WriteToText(
                "assets/processed.csv", shard_name_template=""
            )  # write to file
        )


if __name__ == "__main__":
    run_beam()
