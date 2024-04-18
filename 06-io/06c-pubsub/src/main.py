import apache_beam as beam
from apache_beam.runners.dataflow import DataflowRunner
from apache_beam.runners.direct import DirectRunner
import argparse


def run_beam():
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=str)
    parser.add_argument("--input_subscription", type=str)
    parser.add_argument("--output_topic", type=str)
    args, _ = parser.parse_known_args()

    options = beam.options.pipeline_options.PipelineOptions(streaming=True)
    with beam.Pipeline(options=options) as pipe:
        (
            pipe
            | "Read"
            >> beam.io.ReadFromPubSub(
                subscription=f"projects/{args.project}/subscriptions/{args.input_subscription}",
                with_attributes=True,
            )
            | "Extract string"
            >> beam.Map(
                lambda msg: json.dumps(
                    {"data": msg.data.decode(), "originMessageId": msg.message_id}
                ).encode()  # must be bytestring
            )
            | "Write"
            >> beam.io.WriteToPubSub(
                topic=f"projects/{args.project}/topics/{args.output_topic}"
            )
        )


if __name__ == "__main__":
    run_beam()
