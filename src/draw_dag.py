import apache_beam as beam
from apache_beam.runners.render import RenderRunner
from apache_beam.runners.direct.direct_runner import SwitchingDirectRunner
from apache_beam.runners import DataflowRunner

import csv

from arg import get_args


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


runner_choices = {
    "local": SwitchingDirectRunner(),
    "graph": RenderRunner(),
    "gcp": DataflowRunner(),
}


def get_runner_with_options(args):
    runner = runner_choices.get(args.runner)
    options = beam.options.pipeline_options.PipelineOptions(
        save_main_session=True, streaming=False
    )
    print(options.__dict__)
    return runner, options


def run(args):
    runner, options = get_runner_with_options(args)
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
    args = get_args()
    print(args)
    run(args)
