import apache_beam as beam


class BookTaggingFn(beam.DoFn):
    BANNED = "banned_book"
    ANTIQUE = "antique_book"
    MODERATE = "moderate_book"
    MODERN = "modern_book"

    def process(self, element):
        if element.get("is_banned") is True:
            # update ban status
            if element.get("published_year") <= 1970:
                # 1970 or older + banned --> unbanned
                element["is_banned"] = False
                element["library"] = "Archive common library"
                yield beam.pvalue.TaggedOutput(self.ANTIQUE, element)
            else:
                # later than 1970 --> keep banning
                element["library"] = "Midnight library"
                yield beam.pvalue.TaggedOutput(self.BANNED, element)
        elif element.get("published_year") <= 2017:
            # 2017 or older
            element["library"] = "Central library"
            yield beam.pvalue.TaggedOutput(self.MODERATE, element)
        else:
            element["library"] = "New Bloom library"
            yield beam.pvalue.TaggedOutput(self.MODERN, element)
