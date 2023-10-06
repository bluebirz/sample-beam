import apache_beam as beam

from apache_beam.runners.direct import DirectRunner
from CSVToDict import CSVToDictFn


def run_beam():
    schema = [
        # tuple[str, Callable] --> e.g. `int` for cast to int as `int()`
        ("id", int),
        ("first_name", str),
        ("last_name", str),
        ("gender", str),
        ("occupation", str),
    ]
    with beam.Pipeline(
        runner=DirectRunner(),
        options=beam.options.pipeline_options.PipelineOptions(),
    ) as pipe:
        (
            pipe
            | "Read CSV" >> beam.io.ReadFromText("assets/mock.csv", skip_header_lines=1)
            | "Map CSV to dict" >> beam.ParDo(CSVToDictFn(schema))
            | beam.Map(print)
        )


if __name__ == "__main__":
    run_beam()
