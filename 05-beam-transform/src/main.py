import apache_beam as beam
from apache_beam.runners.direct import DirectRunner


from CSVToDict import CSVToDictFn


def custom_print(elem):
    print(f"color: {elem[0]}")
    for e in elem[1]:
        print(f"\t{e}")


schema = [
    # tuple[str, Callable] --> e.g. `int` for cast to int as `int()`
    ("id", int),
    ("first_name", str),
    ("last_name", str),
    ("gender", str),
    ("occupation", str),
    ("team", str),
    ("age", int),
]
genders = ["M", "F"]
input_file = "assets/people.csv"


def run_beam_part1():
    with beam.Pipeline(
        runner=DirectRunner(),
        options=beam.options.pipeline_options.PipelineOptions(),
    ) as pipe:
        (
            pipe
            | "Read CSV" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | "Map CSV to dict" >> beam.ParDo(CSVToDictFn(schema))
            | "Filter" >> beam.Filter(lambda l: l["gender"] == "F")
            | "Group by key" >> beam.GroupBy(lambda l: l["team"])
            | "Print all" >> beam.Map(custom_print)
        )


def run_beam_part2():
    with beam.Pipeline(
        runner=DirectRunner(),
        options=beam.options.pipeline_options.PipelineOptions(),
    ) as pipe:
        all_people = (
            pipe
            | "Read CSV" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | "Map CSV to dict" >> beam.ParDo(CSVToDictFn(schema))
        )

        male_people, female_people = all_people | beam.Partition(
            lambda l, _: genders.index(l["gender"]), len(genders)
        )

        (
            male_people
            | "count male" >> beam.combiners.Count.Globally()
            | "print num male" >> beam.Map(lambda num: print(f"Male count: {num}"))
        )
        (
            female_people
            | "count female" >> beam.combiners.Count.Globally()
            | "print num female" >> beam.Map(lambda num: print(f"Female count: {num}"))
        )


def run_beam_part3():
    with beam.Pipeline(
        runner=DirectRunner(),
        options=beam.options.pipeline_options.PipelineOptions(),
    ) as pipe:
        all_people = (
            pipe
            | "Read CSV" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | "Map CSV to dict" >> beam.ParDo(CSVToDictFn(schema))
        )

        (
            all_people
            | "Split words" >> beam.FlatMap(lambda l: l["occupation"].split())
            | "Extract only words" >> beam.Regex.replace_all(r"[^\w\d]", "")
            | "Lowercase" >> beam.Map(str.lower)
            | "Get only occupation"
            >> beam.Regex.matches(r"[a-z]{4,}(ist|er|or|ian|ant)$")
            | "count distinct" >> beam.combiners.Count.PerElement()
            | beam.Map(print)
        )


def run_beam_part4():
    with beam.Pipeline(
        runner=DirectRunner(),
        options=beam.options.pipeline_options.PipelineOptions(),
    ) as pipe:
        all_people = (
            pipe
            | "Read CSV" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | "Map CSV to dict" >> beam.ParDo(CSVToDictFn(schema))
        )
        (
            all_people
            | "Extract gender & team"
            >> beam.Map(lambda l: (f"{l['team']}-{l['gender']}", l["age"]))
            | beam.CombinePerKey(max)
            | beam.Map(print)
        )


if __name__ == "__main__":
    run_beam_part1()
    run_beam_part2()
    run_beam_part3()
    run_beam_part4()
