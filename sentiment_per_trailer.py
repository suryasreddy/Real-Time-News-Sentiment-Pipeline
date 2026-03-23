import csv
from mrjob.job import MRJob

# compute avg sentiment (-1, 0, 1 values expected) for each trailer
# average_sentiment = sum(sentiment) / number_of_comments
class MRSentimentPerTrailer(MRJob):
    def mapper(self, _, line):
        try:
            # handling newline characters in CSV fields
            row = next(csv.reader([line.replace('\r', '')]))
            # skipping header or empty rows
            if not row or row[0] == "video_id":
                return

            if len(row) < 6:
                return

            title = row[1].strip()

            # commas inside comments shift the sentiment column index
            sentiment_raw = row[-1].strip()

            # skip rows where sentiment is not a number
            if not sentiment_raw.lstrip('-').isdigit():
                return

            sentiment = int(sentiment_raw)
            if sentiment not in (-1, 0, 1):
                return
            if title:
                yield title, (sentiment, 1)
        except (ValueError, StopIteration, csv.Error):
            return

    # compute sentiment averages
    def reducer(self, title, pairs):
        sent_sum = 0
        count = 0
        for s, c in pairs:
            sent_sum += s
            count += c
        avg = (float(sent_sum) / count) if count else 0.0
        yield title, round(avg, 4)

if __name__ == "__main__":
    MRSentimentPerTrailer.run()