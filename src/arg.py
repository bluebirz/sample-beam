import argparse


def get_args() -> argparse.Namespace:
    """
    Obtain arguments from console and return all known ones.

    Returns:
        argparse.Namespace: Known arguments :=
        * --runner
        * --graph
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # parser.add_argument(
    #     "--runner",
    #     "-r",
    #     choices=["local", "graph", "gcp"],
    #     default="local",
    #     help="Local: DirectRunner; graph: RenderRunner; gcp: DataflowRunner",
    # )
    subparser = parser.add_subparsers(
        dest="runner",
        help="local: DirectRunner; graph: RenderRunner; gcp: DataflowRunner",
    )
    subparser.add_parser("local")
    subparser.add_parser("graph").add_argument("--render_output")
    subparser.add_parser("gcp")
    # parser.add_argument(
    #     "input_file",
    #     type=file_handler.validate_file,
    #     default="soup.yaml",
    #     help="YAML input file path",
    # )
    # parser.add_argument(
    #     "--output_folder",
    #     "-o",
    #     required=False,
    #     type=str,
    #     default="./soup",
    #     help="output folder path",
    # )
    # parser.add_argument(
    #     "--run", "-r", action="store_true", default=False, help="set True if actual run"
    # )
    # parser.add_argument(
    #     "--limit", "-l", required=False, type=int, help="Limit URLs for the process"
    # )
    # parser.add_argument(
    #     "--site",
    #     "-s",
    #     choices=["all"]
    #     + [v.value for k, v in SITE.__members__.items() if v != SITE.UNKNOWN],
    #     default="all",
    #     help="Choice of website",
    # )
    # parser.add_argument(
    #     "--disable_logfile",
    #     "-d",
    #     action="store_true",
    #     default=False,
    #     help="set True if disable all log files for test",
    # )
    # parser.add_argument(
    #     "--reverse",
    #     action="store_true",
    #     default=False,
    #     help="set True if reverse order on the list",
    # )

    params, _ = parser.parse_known_args()
    return params
