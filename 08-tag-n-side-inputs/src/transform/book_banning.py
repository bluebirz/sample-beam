import apache_beam as beam


class BookBanMarkingFn(beam.DoFn):
    def process(self, element, banned_book_list):
        element["is_banned"] = element.get("isbn") in banned_book_list
        yield element
