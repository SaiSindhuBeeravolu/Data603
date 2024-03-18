from mrjob.job import MRJob
from spellchecker import SpellChecker
import re

class MRNonEnglishWordCount(MRJob):

    def mapper(self, _, line):
        # Initialize the spell checker
        spell = SpellChecker()

        # Regular expression to match words
        word_pattern = re.compile(r'\b\w+\b')

        # Split the line into words
        words = word_pattern.findall(line)

        # Check each word for non-English words
        for word in words:
            # Check if the word is misspelled (not in English dictionary)
            if word.lower() not in spell:
                yield word.lower(), 1

    def reducer(self, word, counts):
        # Sum the counts for each word
        total_count = sum(counts)
        yield word, total_count

if __name__ == '__main__':
    MRNonEnglishWordCount.run()
