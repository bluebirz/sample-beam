import apache_beam as beam
import json


def run_beam():
    # with open("./assets/sample.json", 'r') as fptr:
    # data = json.load(fptr)

    options = beam.options.pipeline_options.PipelineOptions()
    with beam.Pipeline(options=options) as pipe:
        pipe | beam.io.ReadFromJson("./assets/sample.json")


if __name__ == "__main__":
    run_beam()
