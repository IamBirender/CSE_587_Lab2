#!/usr/bin/env python
"""mapper.py"""

import sys
import re
from nltk.stem.snowball import SnowballStemmer

stemmer = SnowballStemmer("english")

stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

# input comes from STDIN (standard input)
for line in sys.stdin:
    sublines = line.split(",")
    
    for subline in sublines:
        words = subline.split()

        for word in words:
            word = word.lower()
            
            if word in stopwords:
                continue
            else:
                word = re.sub(r'\W+', '', word)
                alphamatch = re.search('[a-zA-Z]', word)
                if alphamatch:
                    print('%s\t%s' % (stemmer.stem(word), 1))