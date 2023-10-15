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
        ("team", str),
    ]
    with beam.Pipeline(
        runner=DirectRunner(),
        options=beam.options.pipeline_options.PipelineOptions(),
    ) as pipe:
        all_records = (
            pipe
            | "Read CSV" >> beam.io.ReadFromText("assets/mock.csv", skip_header_lines=1)
            | "Map CSV to dict" >> beam.ParDo(CSVToDictFn(schema))
        )

        (
            all_records
            | "Filter" >> beam.Filter(lambda l: l["gender"] == "F")
            | "Group by key" >> beam.GroupBy(lambda l: l["team"])
            | "Print all" >> beam.Map(print)
        )


# io.
# Map
# Filter

# ParDo

# FlatMap

# CombinePerKey

# Flatten

# GroupByKey

# CoGroupByKey

# Combine

# Partition

# Count


if __name__ == "__main__":
    run_beam()
