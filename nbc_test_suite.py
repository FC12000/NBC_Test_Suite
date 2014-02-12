#!/bin/python
import urllib2
import json
import logging
import sys
import inspect
from collections import Counter
from operator import itemgetter


log_file = 'nbc_test_suite_output.log'
source_file_name = "files to test.txt"
file_data = []
source_url_base = "http://stream.nbcsports.com/data/mobile/"
source_urls = []

source_buckets = ["replay", "showcase", "spotlight"]  # lowercase buckets only!
source_keys = ["id", "eventId", "iosStreamUrl", "androidStreamUrl", "start", "length", "title", "info", "link", "image", "free", "oc", "sport", "league"]  
#source_urls = ["http://stream.nbcsports.com/data/mobile/featured-2013.json", "http://brandme-prod.s3.amazonaws.com/learfield/weeks/foooooo.json", "http://stream.nbcsports.com/data/mobile/nfl.json", "http://brandme-prod.s3.amazonaws.com/learfield/weeks/zoooooo.json", "http://brandme-prod.s3.amazonaws.com/learfield/weeks/boooooo.json"] # TODO Francis add the files we need. 
#source_urls = ["http://stream.nbcsports.com/data/mobile/featured-oly-2013.json",
    #"https://dl.dropboxusercontent.com/u/256718337/NBC/live-oly_mens_downhill_event_added.json",
    #"https://dl.dropboxusercontent.com/u/256718337/NBC/live-oly_mens_downhill_event_added_duplicate_pid.json"
    #"https://dl.dropboxusercontent.com/u/256718337/NBC/featured-oly-2013_duplicate_pid_13397.json"]


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', filename=log_file, filemode='w')
print "logging to file %s" % log_file

#TODO build useful output a) print to screen b) write to log c) write to csv   
#also get more useful meta data 


#TODO make the whole thing configured 


def open_json_files_list(list_of_files):
    logging.info( "Reading the list of files...")
    with open(list_of_files, 'r') as infile:
        for line in infile:
            file_data.append(line.rstrip('\n'))
    logging.info( "List of files stored in \"file_data\"")
    
    
def build_source_urls(data):
    logging.info( "Building source URLs...")
    for eachFileName in data:
        #source_urls.append(source_url_base + each_file_name)
        source_urls.append(''.join([source_url_base, eachFileName]))
    #print source_urls
    logging.info( "List of source URLs stored in \"source_urls\"")


def test_necessary_files_are_present(): 
    #Response building....
    currentTestName = inspect.stack()[0][3]
    logging.info("Calling test_necessary_files_are_present()")
        
    for url in source_urls: 
        #Response building....
        print "Running tests for URL: %s" % url
        try: 
            response = urllib2.urlopen(url)
            logging.info("\n\nSUCCESS: url %s does exist.  Continuing test suite" % url)
            contents = response.read()
            test_source_json = json.loads(contents)
            #Check if the file is empty before running tests
            if len(test_source_json):
                test_run_valid_url_tests(test_source_json, url)
            else:
                logging.warning("The following file is empty: %s" % url)
        except urllib2.HTTPError, e:
            logging.error("")
            logging.error("FAIL: url %s does not exist." % url )
            logging.error("Caught exception %s for url %s " % (e.code, url) )
        except: 
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logging.exception("caught exception %s, and obj %s at line number %s" % (exc_type, exc_obj, exc_tb.tb_lineno))
            #logging.info("FAIL: url %s does not exist." % url)
            continue
    
    
def test_run_valid_url_tests(test_source_json, url):
    test_has_duplicate_pid(test_source_json, url)
    #test_event_order(test_source_json, url)
    #test_buckets_are_missing(test_source_json, url)
    #test_new_keys_present_in_old_source(test_source_json, url)
    #test_old_keys_present_in_new_source(test_source_json, url)
    #test_duplicates(json_source_data, url, test_type)
    #test_compare_source_values(test_source_json, url)
    
    
def test_has_duplicate_pid(test_source_json, url):
    currentTestName = inspect.stack()[0][3]
    
    logging.info("Calling %s" % currentTestName)

    try:
        duplicatePidFound = False
        eventPids = []
        pidCount = Counter()
        mostCommonPids = []
        
        if isinstance(test_source_json, dict):
            #print "The file has keys: %s" % test_source_json.keys()
            eventPids = []
            for bucketLabel, eventList in test_source_json.iteritems():
                if bucketLabel != "showCase": # Ignore data in the showCase bucket
                    
                    # Grab the pid, eventId, and bucket name for each event and put them in an array
                    for event in eventList:
                        if "pid" in event:
                            eventPids.append((bucketLabel, event["pid"], event["eventId"], event["title"]))

            # Tally the occurrences of the pids in the list for each bucket
            for eachEvent in eventPids:
                pidCount[eachEvent[1]] += 1

            # Return the 3 most common pids for each bucket
            #print "The 3 most common pids are: %s" % (pidCount.most_common(3))
                
            # Determine if any pids have a tally greater than 1 for each bucket
            for eachPid in pidCount:
                if pidCount[eachPid] > 1:
                    # Duplicate pid found! Go through the eventPid list and return the pids with their associated bucket, eventId, and titles
                    logging.error("Duplicate pid's found!")
                    duplicatePidFound = True
                    for eachEvent in eventPids:
                        if eachEvent[1] == eachPid:
                            logging.error("bucket: %s, pid: %s, eventId: %s, title: %s" % (eachEvent[0], eachEvent[1], 
                                eachEvent[2], eachEvent[3]))
            if not duplicatePidFound:
                print "There are NO duplicate pids."
                
        elif isinstance(test_source_json, list):
            #print "The file does NOT have keys."
            
            # Grab the pid and eventId's for each event and put them in an array
            for event in test_source_json:
                if "pid" in event:
                    eventPids.append((event["pid"], event["eventId"], event["title"]))
            
            # Tally the occurrences of pids in the list
            for eachEvent in eventPids:
                pidCount[eachEvent[0]] += 1
            
            # return most common 3 pids
            mostCommonPids = pidCount.most_common(3)
            
            # Determine if any pids have a tally greater than 1
            #print "3 most common pids are: ", mostCommonPids
            for eachPid in pidCount:
                if pidCount[eachPid] > 1:
                    # Duplicate pid found! Go through the eventPid list and return the pids with their associated eventIds.
                    logging.error("Duplicate pid's found!")
                    duplicatePidFound = True
                    for eachEvent in eventPids:
                        if eachEvent[0] == eachPid:
                            logging.error("pid: %s, eventId: %s" % (eachEvent[0], eachEvent[1], eachEvent[2]))
            if not duplicatePidFound:
                print "There are NO duplicate pids."    
            
        else:
            print "The file is not a dictionary or does NOT have keys."
        
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.exception("caught exception %s, and obj %s at line number %s" % (exc_type, exc_obj, exc_tb.tb_lineno))   
    
    
def test_event_order(test_source_json, url):
    jsonStartValues = list()
    currentTestName = inspect.stack()[0][3]

    logging.info("Calling test_event_order(test_source_json, url)")
    # Get the start times for each event in the JSON file.
    try:
        for bucketLabel, eventList in test_source_json.iteritems(): 
            if bucketLabel == "spotlight": #then we care about and want to loop through it again 
                index = 0
                for event in eventList: 
                    #Compare the start time of the current event with the start time of the next event
                    index = index + 1
                    if index < len(eventList):
                        if event["start"] < eventList[index]["start"]:
                            #oh crap it broke. now what?
                            #logging.info("URL: %s" % url)
                            # current event with eventId is newer than the next event with eventId
                            logging.exception("FAIL: Current Event (eventId: %s, start: %s) is newer than "
                                "the Next Event (eventId: %s, start: %s)" % (event["eventId"], event["start"], 
                                eventList[index]["eventId"], eventList[index]["start"]))
                        else:
                            #logging.info("Current Event (eventId: %s) is older than the Next Event (eventId: %s). Test passed." % (event["eventId"], eventList[index]["eventId"]))
                            continue
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.exception("caught exception %s, and obj %s at line number %s" % (exc_type, exc_obj, exc_tb.tb_lineno))
    
def test_buckets_are_missing(test_source_json, url):
    jsonBuckets = []
    currentTestName = inspect.stack()[0][3]

    logging.info("calling test_buckets_are_missing(test_source_json)")

    # Get the buckets in the JSON file.       
    for bucketLabel in test_source_json:
        #print "url: %s, bucket: %s" % (url, bucketLabel)
        #print isinstance(bucketLabel, str)
        #print type(bucketLabel)
        if isinstance(bucketLabel, basestring):
            jsonBuckets.append(bucketLabel.lower())
        else:
            logging.error("FAIL: IN TEST %s: the file, %s, does not contain buckets." % (currentTestName, url))
            return

    missingBuckets = list(set(source_buckets) - set(jsonBuckets))
    if missingBuckets:
        #print "length of missingBuckets: %s" % len(missingBuckets)
        logging.error("FAIL: IN TEST %s: the file, %s, is missing the following buckets: %s" % (currentTestName, 
            url, missingBuckets))


def test_duplicates(test_source_json, url, test_type):
    #TODO check if there are duplicates.  (also need to know how to define a dup)
    pass 

    
def test_compare_source_values(test_source_json, url):    
    #TODO Compare values (aka pattern matching) a) compare old vs new b) compare new vs new 
    pass 
    

def test_new_keys_present_in_old_source(test_source_json, url): 
    currentTestName = inspect.stack()[0][3]

    logging.info("calling test_new_keys_present_in_old_source(test_source_json)")
    
    for bucketLabel, eventList in test_source_json.iteritems(): 
        if bucketLabel == "spotlight": #then we care about and want to loop through it again 
            for event in eventList: 
                for key in event: 
                    if key not in source_keys: 
                        logging.error("FAIL: IN TEST test_new_keys_present_in_old_source: key '%s' was NOOOOT found "
                            "in source keys for the following event:\n 'eventId': %s\n 'title': %s" % (key, 
                            event["eventId"], event["title"]))
                        

def test_old_keys_present_in_new_source(test_source_json, url): 
    currentTestName = inspect.stack()[0][3]

    logging.info("calling test_old_keys_present_in_new_source(test_source_json)")
    
    for bucketLabel, eventList in test_source_json.iteritems(): # loop through the. first layer of the the json i got back.  bucket label represents the three categories: spotlight, showCase, replay.  event list represents the list of events in each fo the buckets
        if bucketLabel == "spotlight": #then we care about and want to loop through it again 
            for event in eventList: 
                for key in source_keys: 
                    # ignore the "image" key since that node does not exist in the new files yet
                    if key != "image" and key not in event: 
                        logging.error("FAIL: IN TEST test_old_keys_present_in_new_source: key '%s' was NOOOOT found "
                            "in source keys for the following event:\n 'eventId': %s\n 'title': %s" % (key, 
                            event["eventId"], event["title"]))
                        
                        
def build_results(responseDictionary):
    #add meta 
    #jsonData = json.dumps(responseDictionary, sort_keys=True, separators=(',', ': '))

    prettyPrint = 1
    #logging.error(" ")
    #logging.info(json.dumps(responseDictionary))
    with open("data.json", 'w') as outfile:
         if prettyPrint == 1:
             outfile.write(json.dumps(responseDictionary, sort_keys = True, indent = 4, separators=(',', ': '), ensure_ascii=True))
         else: 
             outfile.write(json.dumps(responseDictionary, sort_keys=True, separators=(',', ': ')))
         outfile.flush()
         print "Write to file errors: %s" % outfile.errors
    print "Error results written to: data.json"


if __name__ == "__main__":
    open_json_files_list(source_file_name) #list of JSON files stored in file_data
    build_source_urls(file_data) #list full URLS stored in source_urls
    
    test_necessary_files_are_present()
    #build_results(responseDictionary)
    logging.info("Tests are completed.")
