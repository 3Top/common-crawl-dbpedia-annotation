import gzip
#
import boto
import warc
#
from boto.s3.key import Key
from gzipstream import GzipStreamFile
from mrjob.job import MRJob, MRStep


class CCJob(MRJob):
    def process_record(self, record):
        """
        Override process_record with your mapper
        """
        raise NotImplementedError('Process record needs to be customized')

    def mapper_get_words(self, _, line):
        f = None
        ## If we're on EC2 or running on a Hadoop cluster, pull files via S3
        if self.options.runner in ['emr', 'hadoop']:
            # Connect to Amazon S3 using anonymous credentials
            conn = boto.connect_s3(anon=True)
            pds = conn.get_bucket('aws-publicdatasets')
            # Start a connection to one of the WARC files
            k = Key(pds, line)
            f = warc.WARCFile(fileobj=GzipStreamFile(k))
        ## If we're local, use files on the local file system
        else:
            print 'Loading local file {}'.format(line)
            f = warc.WARCFile(fileobj=gzip.open(line))
        ###
        for i, record in enumerate(f):
            for word_or_pair, count in self.process_record(record):
                yield word_or_pair, count
            self.increment_counter('commoncrawl', 'processed_records', 1)

    # TODO: Make the combiner use the reducer by default
    def combiner_count_words(self, word, count):
        yield word, sum(count)

    def reducer_count_words(self, word, count):
        yield word, sum(count)
        
    def reducer_filter(self, word, count):
        if count > 100:
            yield word, count

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_filter)    
        ]
        
if __name__ == '__main__':
    CCJob.run()