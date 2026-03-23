import csv
from mrjob.job import MRJob
from mrjob.step import MRStep

# trailer (video_title) with the highest number of comments is outputed in text file
class MRTopCommentedTrailer(MRJob):
    def mapper(self, _, line):
        try:
            row = next(csv.reader([line]))

            # skipping header
            if not row or row[0] == "video_id":
                return

            # skip rows with no video_id or video_title
            if len(row) < 2:
                return

            title = row[1].strip()
            if title:
                yield title, 1

        except Exception:
            return

    def reducer_count(self, title, counts):
        yield title, sum(counts)

    def mapper_to_final(self, title, count):
        # send all (title, count) pairs to one reducer to find the max
        yield "FINAL", (title, count)

    def reducer_final(self, _, items):
        top_title = None
        top_count = 0

        # comparing each trailer's count for max
        for title, count in items:
            if count > top_count:
                top_count = count
                top_title = title

        # output most commented as readable dictionary
        yield "Top_Commented_Trailer", {"video_title": top_title, "comment_count": top_count}

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_count),
            MRStep(mapper=self.mapper_to_final, reducer=self.reducer_final),
        ]

if __name__ == "__main__":
    MRTopCommentedTrailer.run()