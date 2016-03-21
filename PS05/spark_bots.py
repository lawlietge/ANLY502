#!/usr/bin/env python2

import pytest
import struct
import socket
from pyspark import SparkContext

def ip2int(addr):                                                               
    return struct.unpack("!I", socket.inet_aton(addr))[0]                       

def int2ip(addr):                                                               
    return socket.inet_ntoa(struct.pack("!I", addr))  


# Takes the network number, the netmask, and the IP address to match,
# and report if they match. 
def netmatch( network, netmask, ipaddress ):
    bitmask = (0xffffffff ^ (0xffffffff >> netmask))
    return (network & bitmask) == (ipaddress & bitmask)

# This does the same, but with the network in cidr notation, and the ipaddress
# as a dotted quad:
def netmatch_str( cidr, ipaddress_str ):
    ( network_str, netmask_str ) = cidr.split("/")
    network = ip2int( network_str )
    netmask = int( netmask_str )
    return netmatch( network, netmask, ip2int( ipaddress_str ))
    
# We will have a database of (network, netmask, country) tuples.
# The database is a sorted array.
# This function searches the database and returns the country or "" if there is no match
# The search is done with the IP address as a dotted-quad
def geo_search( db, ipaddr_str ):
    import bisect
    ipaddr_int = ip2int( ipaddr_str )
    idx = bisect.bisect_left( db, (ipaddr_int,0,""))
    idx -= 1                    # because the one to the left is the match!
    return db[idx][2] if netmatch( db[idx][0], db[idx][1], ipaddr_int ) else ""




################################################################
## Code for parsing Apache weblogs
## This is an improved parser that's tolerant of bad data.
## Instead of throwing an error, it return a Row() object
## with all NULLs
from pyspark.sql import Row
import dateutil, dateutil.parser, re

APPACHE_COMBINED_LOG_REGEX = '([(\d\.)]+) [^ ]+ [^ ]+ \[(.*)\] "(.*)" (\d+) [^ ]+ ("(.*)")? ("(.*)")?'
WIKIPAGE_PATTERN = "(index.php\?title=|/wiki/)([^ &]*)"

appache_re  = re.compile(APPACHE_COMBINED_LOG_REGEX)
wikipage_re = re.compile(WIKIPAGE_PATTERN)



def parse_apache_log_line(logline):
    from dateutil import parser
    m = appache_re.match(logline)
    if m==None: return Row(ipaddr=None, timestamp = None, request = None, result = None,
                           user=None, referrer = None, agent = None, url = None, datetime = None,
                           date = None, time = None, wikipage = None)
                           
    timestamp = m.group(2)
    request   = m.group(3)
    agent     = m.group(7).replace('"','') if m.group(7) else ''

    request_fields = request.split(" ")
    url         = request_fields[1] if len(request_fields)>2 else ""
    datetime    = parser.parse(timestamp.replace(":", " ", 1)).isoformat()
    (date,time) = (datetime[0:10],datetime[11:])

    n = wikipage_re.search(url)
    wikipage = n.group(2) if n else ""

    return Row( ipaddr = m.group(1), timestamp = timestamp, request = request,
        result = int(m.group(4)), user = m.group(5), referrer = m.group(6),
        agent = agent, url = url, datetime = datetime, date = date,
        time = time, wikipage = wikipage)
################################################################

#
# This function expects an array of results where each result
# has the format ((country,wikipage), count)
def print_most_popular_pages_per_country(results):
    def print_top_3(counts_for_country):
        if not current_country: return
        import unicodedata
        print("{}:".format(unicodedata.normalize('NFKD',current_country).encode('ascii','ignore')))
        i = 0
        for (count,wikipage) in sorted(counts_for_country,reverse=True):
            if wikipage=="": continue # don't print empty pages
            if wikipage=="-": continue
            print("  {:30} {:-5}".format(wikipage,count))
            i += 1
            if i==3: break
        print("\n")
            
    current_country = None
    counts_for_country = []
    for result in results:
        country  = result[0][0]
        wikipage = result[0][1]
        count    = result[1]
        if country != current_country:
            print_top_3(counts_for_country)
            counts_for_country = []
            current_country = country
        counts_for_country.append((count,wikipage))
    print_top_3(counts_for_country)



if __name__=="__main__":
    sc = SparkContext( appName = 'geolocate' )

    # a. Read the two files and perform an RDD join operation, so that each network prefix is joined with its country
    # 
    # Filter out the first line and split each line into fields
    geolite_ipblocks_raw = sc.textFile("s3://gu-anly502/maxmind/GeoLite2-Country-Blocks-IPv4.csv")
    header               = geolite_ipblocks_raw.first()
    geolite_ipblocks     = geolite_ipblocks_raw.\
                           filter(lambda line:line != header).\
                           map(lambda line:line.split(","))
    # Create a key,value RDD where the key is the country name
    geolite_ipblocks_by_countryid = geolite_ipblocks.map(lambda x:(x[1],x))


    # Get the location names

    # Note that this file is so small, we could just do it all in
    # memory, but we're doing it with an RDD for completeness
    geolite_locations_raw  = sc.textFile("s3://gu-anly502/maxmind/GeoLite2-Country-Locations-en.csv")
    header                 = geolite_locations_raw.first()
    geolite_locations      = geolite_locations_raw.filter(lambda line:line != header)
    geolite_locations2     = geolite_locations.map(lambda line:line.split(","))
    geolite_locations_by_countryid = geolite_locations2.map(lambda x: ( x[0], x[5] ))

    geolite = geolite_ipblocks_by_countryid.join(geolite_locations_by_countryid)
    
    # geolite now looks like (countryid, ([geolite locations], countryname)]
    # Use a function that extracts the network CIDR block and the country name and returns as a list
    def extract(row):
        (countryid,join_result) = row
        geolite_ipblock = join_result[0]  # A line from the geolite_ipblocks
        country = join_result[1]          # the country
        ( network_str, netmask_str ) = geolite_ipblock[0].split("/")
        return ( ip2int(network_str), int(netmask_str), country) 
    
    # We want to do a binary search, so after we map, we sort by the network ID:

    networks_and_locations = geolite.map(extract).sortBy(lambda vals:vals[0])

    # Here's what it looks like:
    #
    # In [73]: networks_and_locations.take(3)
    #
    # Out [73]: [(692756480, 20, u'Rwanda'),
    #            (692953088, 22, u'Rwanda'),
    #            (696930304, 21, u'Rwanda')]

    # We're going to be using this a lot, so let's distribute it to all of the nodes
    networks_and_locations_bc = sc.broadcast(networks_and_locations.collect())

    # networks_and_locations_bc is now on all of the nodes. 
    # we can use networks_and_locations_bc.value to get its value

    # Read the 2012 forensicswiki data into an RDD. 
    forensicswiki_raw = sc.textFile("s3://gu-anly502/ps03/forensicswiki.2012.txt")

    # Parse the lines and filter out those without an IP address
    forensicswiki = forensicswiki_raw.map(parse_apache_log_line).filter(lambda row: row['ipaddr']!=None)

    # Generate a RDD that has the ipaddress, the wikipage, and the country
    forensicswiki_res = forensicswiki.map(lambda row: (row['ipaddr'],row['wikipage'],geo_search(networks_and_locations_bc.value,row['ipaddr'])))

    # In [243]: forensicswiki_res.take(3)
    # Out[243]: 
    # [(u'77.21.0.59', u'Write_Blockers', u'Germany'),
    # (u'77.21.0.59', '', u'Germany'),
    # (u'77.21.0.59', '', u'Germany')]
    
    # "For each country, determines the three most popular wiki pages."
    # So for each country lets get a list of all the pages and their counts.
    # We'll make an RDD that looks like ((Country, Page), 1) and then reduce it with add
    forensicswiki_counts = forensicswiki_res.map(lambda x: ((x[2],x[1]),1))

    import operator
    forensicswiki_sums = forensicswiki_counts.reduceByKey(operator.add)

    # forensicswiki_sums.take(3):
    # 

    # In [257]: forensicswiki_sums.take(3)
    # Out[257]: 
    # [((u'Germany',
    #   u'How+Pennsylvania+businesses+can+shop+for+a+lower+rate+for+their+organization+'),
    #  4),
    # ((u'Ireland', u'Special:RecentChangesLinked/Volatile_Systems'), 1),
    # ((u'Germany', u'Dealing+With+Bankruptcy'), 1)]

    # Okay, looks like we can get the countries easily:
    results = forensicswiki_sums.sortByKey(lambda x:x[0]).collect()

    # We can actually process this in a single loop:
    print_most_popular_pages_per_country(results)

        
