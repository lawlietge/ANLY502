#!/usr/bin/env python2

# To get started with the join, 
# try creating a new directory in HDFS that has both the fwiki data AND the maxmind data.

import mrjob
from mrjob.job import MRJob
from weblog import Weblog       # imports class defined in weblog.py
import os

class FwikiMaxmindJoin(MRJob):
    SORT_VALUES=True
    def mapper(self, _, line):
        # Is this a weblog file, or a MaxMind GeoLite2 file?
        filename = mrjob.compat.jobconf_from_env("map.input.file")
        if "top1000ips_to_country.txt" in filename:
            countryfields = line.split("\t")
            self.increment_counter("Info","Country Count",1)
            # Handle as a GeoLite2 file
            #
            yield countryfields[0],("Country",countryfields[1])
            
        else:
            log=Weblog(line)
            logfields = (log.ipaddr,log.datetime,log.url,log.wikipage())
            self.increment_counter("Info","Weblog Count",1)
            # Handle as a weblog 
            yield logfields[0],("Weblog",logfields)
        
        # output <date,1>
        # yield CHANGEME


    def reducer(self, key, values):
        country=None
        for v in values:
            if len(v)!=2
                self.increment_counter("Warn","Invalid Join",1)
                continue
            if v[0]=='Country':
                country=v[1]
                continue
            if v[0]=='Weblog':
                ip=v[1]
                if country:
                    yield ip[0],country
                else:
                    self.increment_counter("Warn","weblog without country")

    def sum_mapper(self, word, count):
        # notice that we put the counts first!
        yield count,1

    def sum_reducer(self, key, values):
        # put code here to select the top 10 values for the key and output them
	yield key,sum(values)

    def sort_mapper(self, word, count):
        # notice that we put the counts first!
        yield "Top10", (word,count)

    def sort_reducer(self, key, values):
        # put code here to select the top 10 values for the key and output them
        values=sorted(values)
		return values
    
    
    def steps(self):
        return[
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.sum_mapper,
                   reducer=self.sum_reducer),
            MRStep(mapper=self.sort_mapper,
                   reducer=self.sort_reducer)]


if __name__=="__main__":
    FwikiMaxmindJoin.run()