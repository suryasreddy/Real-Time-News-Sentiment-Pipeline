import csv
import re
from mrjob.job import MRJob

# regex pattern to identify alphabetic words only, ensuring clean keywords from comments
WORD = re.compile(r"[a-zA-Z]+")

# find most frequently used keywords from all trailer comments using processed_comment column
class MRTrendingKeywords(MRJob):

    def mapper(self, _, line):
        try:
            row = next(csv.reader([line]))

            # skipping header
            if not row or row[0] == "video_id":
                return

            # processed_comment column
            text = row[4].lower()

            # find 3+ characters long words that appear
            # filter small common words like "a", "of", etc
            for w in WORD.findall(text):
                if len(w) >= 3:
                    yield w, 1

        except Exception:
            return

    # summing value pair with word
    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == "__main__":
    MRTrendingKeywords.run()