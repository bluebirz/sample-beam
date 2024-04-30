import apache_beam as beam
from transform.csv_to_dict import CSVToDictFn
from transform.book_banning import BookBanMarkingFn
from transform.book_tagging import BookTaggingFn
from transform.dict_to_csv import DictToCsvFn

schema = [
    # tuple[str, Callable] --> e.g. `int` for cast to int as `int()`
    ("id", str),
    ("isbn", str),
    ("title", str),
    ("author", str),
    ("published_year", int),
]
input_file = "assets/books.csv"
side_input_file = "assets/banned_book.txt"

updated_schema = [
    "id",
    "isbn",
    "title",
    "author",
    "published_year",
    "is_banned",
    "library",
]
updated_banned_book = "outputs/updated_banned_book.csv"
updated_antique_book = "outputs/updated_antique_book.csv"
updated_moderate_book = "outputs/updated_moderate_book.csv"
updated_modern_book = "outputs/updated_modern_book.csv"


def mapToCSVRow(elem: dict) -> str:
    return ",".join(f'"{v}"' for k, v in elem.items())


def run_beam():
    options = beam.options.pipeline_options.PipelineOptions()
    with beam.Pipeline(options=options) as pipe:
        # prepare side inputs
        banned_book_list = pipe | "Get side input" >> beam.io.ReadFromText(
            side_input_file
        )
        # main ops
        books = (
            pipe
            | "Read CSV" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | "Map CSV to dict" >> beam.ParDo(CSVToDictFn(schema))
            | "Mark banned book"
            >> beam.ParDo(BookBanMarkingFn(), beam.pvalue.AsIter(banned_book_list))
            | "Tagging"
            >> beam.ParDo(BookTaggingFn()).with_outputs(
                BookTaggingFn.BANNED,
                BookTaggingFn.ANTIQUE,
                BookTaggingFn.MODERATE,
                BookTaggingFn.MODERN,
            )
        )
        # write banned books
        (
            books[BookTaggingFn.BANNED]
            | "prep schema for banned book" >> beam.ParDo(DictToCsvFn(updated_schema))
            | "Write to ban.csv"
            >> beam.io.WriteToText(
                updated_banned_book,
                header=",".join(updated_schema),
                shard_name_template="",
            )
        )
        # write antique books
        (
            books[BookTaggingFn.ANTIQUE]
            | "prep schema for antique book" >> beam.ParDo(DictToCsvFn(updated_schema))
            | "Write to antique.csv"
            >> beam.io.WriteToText(
                updated_antique_book,
                header=",".join(updated_schema),
                shard_name_template="",
            )
        )
        # write moderate books
        (
            books[BookTaggingFn.MODERATE]
            | "prep schema for moderate book" >> beam.ParDo(DictToCsvFn(updated_schema))
            | "Write to moderate.csv"
            >> beam.io.WriteToText(
                updated_moderate_book,
                header=",".join(updated_schema),
                shard_name_template="",
            )
        )
        # write modern books
        (
            books[BookTaggingFn.MODERN]
            | "prep schema for modern book" >> beam.ParDo(DictToCsvFn(updated_schema))
            | "Write to modern.csv"
            >> beam.io.WriteToText(
                updated_modern_book,
                header=",".join(updated_schema),
                shard_name_template="",
            )
        )


if __name__ == "__main__":
    run_beam()
