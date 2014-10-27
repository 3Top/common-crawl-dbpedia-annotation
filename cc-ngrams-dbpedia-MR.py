from __future__ import division
from collections import Counter
import word2phrase
from itertools import tee, izip_longest
from collections import defaultdict
import gzip
from gzipstream import GzipStreamFile
import warc
#import boto
from mrcc import CCJob
from mrjob.job import MRJob
import os

#nb iterations. More times gives longer phrases
nb_iter = 3
# pairs of words w/ score >= threshold are marked as phrases
threshold = 100
# % of initial value by which to decrement threshold with each iter
discount = 0.05
#nb of times a phrase should appear together to be considered
min_count = 100   
#separator 
sep = '_'

class WordCount1(CCJob):
  """ One mapper for each line """
  def process_record(self, record):
    if record['Content-Type'] != 'text/plain':
        return
    page = record.payload.read()
    for line in page.split('\n'):
#         vocab = defaultdict(int)
        train_words = 0
        for pair in pairwise(line.split()):
            yield pair[0], 1
            if None not in pair:
                yield pair, 1
            train_words += 1
            self.increment_counter('commoncrawl', 'processed_pages', 1)    
    
class WordCountS(CCJob):
  """ One mapper for one Segment """
  def process_record(self, record):
      f = warc.WARCFile(fileobj=gzip.open(filepath))
      vocab = defaultdict(int)
      nbwords = 0
      for i, record in enumerate(f):
          devnull = open(os.devnull,"w")
          if record['Content-Type'] != 'text/plain':
                continue
          page = record.payload.read()        
          v, n = learn_vocab_from_train_iter(page)               
          yield v, n
      self.increment_counter('commoncrawl', 'processed_pages', 1)

def process_wet_file(filepath):    
    print 'Loading local file {}'.format(filepath)
    f = warc.WARCFile(fileobj=gzip.open(filepath))
    vocab = defaultdict(int)
    nbwords = 0
    for i, record in enumerate(f):
#         devnull = open(os.devnull,"w")
#         if record['Content-Type'] != 'text/plain':
#             continue
#         page = record.payload.read()        
#         v, n = learn_vocab_from_train_iter(page)
        vocab.update(v)
        nbwords += n
    print "filtering the vocab..."
    vocab = filter_vocab(vocab, min_count)
#     print "writing the filtered vocab to disk..."    
#     for row in words:
# #         print row
#         devnull.write(' '.join(row) + '\n')
#     devnull.close()
    print "Done."
    print "nb pages: %d" % i
    print "nb words: %d" % nbwords
    

def pairwise(iterable):
    """
    Adjacent pairs with overlap
    >>> list(pairwise(range(5)))
    [(0, 1), (1, 2), (2, 3), (3, 4), (4, None)]
    """
    a, b = tee(iterable)
    next(b)
    return izip_longest(a, b)

def learn_vocab_from_train_iter(train_iter):
    """
    Creates a frequency table mapping unigrams and bigrams to their
    count of appearances in the training set
    Analogous to LearnVocabFromTrainFile from original
    """
    vocab = defaultdict(int)
    train_words = 0
    for line in train_iter:
        for pair in pairwise(line):
            vocab[pair[0]] += 1
            if None not in pair:
                vocab[pair] += 1
            train_words += 1
    return vocab, train_words


def filter_vocab(vocab, min_count):
    """
    Filter elements from the vocab occurring fewer than min_count times
    """
    return dict((k, v) for k, v in vocab.iteritems() if v >= min_count)


def train_model(train_iter, min_count=5, threshold=100.0, sep='_'):
    """
    The whole file-to-file (or in this case iterator to iterator) enchilada
    Does the same thing as Mikolov's original TrainModel in word2phrase.c
    Parameters:
    train_iter : an iterator containing tokenized documents
        Example: [['hi', 'there', 'friend'],['coffee', 'is', 'enjoyable']]
    min_count : min number of mentions for filter_vocab to keep word
    threshold : pairs of words w/ score >= threshold are marked as phrases.
        In word2phrase.c this meant the whitespace separating
        two words was replaced with '_'

    Yields:
        One list per row in input train_iter
    """
    vocab_iter, train_iter = tee(train_iter)
    vocab, train_words = learn_vocab_from_train_iter(vocab_iter)
    print "Done Vocab", len(vocab), train_words
    vocab = filter_vocab(vocab, min_count)
    print "Filtered Vocab", len(vocab)

    for line in train_iter:
        out_sentence = []
        pairs = pairwise(line)
        for pair in pairs:
            pa = vocab.get(pair[0])
            pb = vocab.get(pair[1])
            pab = vocab.get(pair)

            if all((pa, pb, pab)):
                score = (pab - min_count) / pa / pb * train_words
            else:
                score = 0.0
            if score > threshold:
                next(pairs)
                out_sentence.append(sep.join(pair))
            else:
                out_sentence.append(pair[0])
        yield out_sentence

#process_wet_file('/home/3TOP/fscharf/workspace/common-crawl-word-embedding/cc-mrjob/common-crawl/crawl-data/CC-MAIN-2014-35/segments/1408500800168.29/wet/CC-MAIN-20140820021320-00000-ip-10-180-136-8.ec2.internal.warc.wet.gz')
if __name__ == '__main__':
    WordCount1.run()
    