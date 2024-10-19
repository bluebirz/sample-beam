import apache_beam as beam
import json


def custom_combine(input):
    print(input)
    yield input


def run_beam():
    with open("./assets/sample.json", "r") as fptr:
        data = json.load(fptr)

    options = beam.options.pipeline_options.PipelineOptions()
    with beam.Pipeline(options=options) as pipe:
        proc = (
            pipe
            | beam.Create(data)
            # | beam.Map(lambda d: beam.window.TimestampedValue(d, d.get("timestamp")))
            # | beam.WindowInto(beam.window.FixedWindows(3))
            # | beam.CombinePerKey(lambda d: d.get("alphabet"))
            # | beam.MapTuple(lambda k, v: (k, v))
            # | beam.CombineValues(lambda t: "".join(t))
            | beam.Map(lambda d: (d["alphabet"], d["timestamp"]))
            | beam.CombinePerKey(custom_combine)
        )
        proc | beam.Map(print)


if __name__ == "__main__":
    run_beam()
