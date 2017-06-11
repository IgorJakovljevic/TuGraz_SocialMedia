import nltk
import csv
import pymysql
import MySQLdb
import sys
import string
import pickle
import io

#crawled_tags = ["runtastic", "running", "gym", "workout"]

hostname = 'localhost'
username = 'root'
password = ''
database = 'twitterdata'

#tag_dict[crawled_tag][other_tag] = counter of occurrences
tag_dict = {}

#key words for keyword based filtering
keywords = {'healthy': ['health', 'healthy'], 
    'active': ['doing', 'workout', 'playing', 
                'hardwork', 'fit', 'active', 'fitness'], 
    'passive': ['watching', 'beer', 'tv', 'couch', 
                'lazy', 'superbowl', 'wm', 'cheering', 
                'relax', 'relaxing'],
    'food': ['food', 'eating', 'eat', 'meal', 'cook', 'cooking', 'yummy', 
                'foodporn', 'dinner', 'breakfast', 'delicious', 'lunch',
                'restaurant', 'veggies', 'fruit', 'vegetables', 'nom', 'nomnom',
                'foodgasm', 'fruits', 'healthyfood']}
                
                
keyword_counter = {}

def expandTable(col_name, conn):
    cur = conn.cursor()
    
    '''try:
        cur.execute("SHOW COLUMNS FROM " + col_name + " LIKE 'fieldname'")
        for current in cur.fetchall():
            print current
    except:
        print "not here"'''
    
    cur.execute("SHOW COLUMNS FROM `tweets` LIKE '" + col_name + "'")   
    fetched = cur.fetchall()
    if len(fetched) == 0:
        print "Creating column ", col_name # if col not there, create it
        cur.execute("ALTER TABLE tweets ADD " + col_name + " VARCHAR(150)")
    else:
        print "Column already existing"
        #print fetched
        


def doQuery(conn, writeIntoDB):
    cur = conn.cursor()

    cur.execute("SELECT id, Hashtag, Text, created_at FROM tweets")
    #cur.execute("SELECT id, Hashtag, Text FROM tweets WHERE id = 269")
    
    entries = cur.fetchall()
    
    #checkDate(entries)
    
    #print entries[0][0] #id
    #print entries[0][1] #HashTag
    #print entries[0][2] #Text
    #sys.exit()

    for entry in entries:
 
        if entry[1] not in tag_dict:
            tag_dict[entry[1]] = {}
        #get all hashtags
        tag_list = entry[2].split("#")
        tag_list.pop(0)
        #print tag_list
        all_hashtags = []
        
        
        #remove remaining text from hashtags
        for tag in tag_list:
 
            helper_list = tag.split(" ", 1)
            helper_list[0] = helper_list[0].lower().translate(None, string.punctuation)
            
            #remove all the newlines
            if "\n" in helper_list[0]:
                helper_list = helper_list[0].split("\n", 1)
                #print helper_list[0]
            #helper_list[0].replace("...", "")
            if entry[1] != helper_list[0]:
                #print "we are unequal"
                #print current_tag
                #print helper_list[0].lower()
                
                if helper_list[0] != '':
                    if helper_list[0] not in tag_dict[entry[1]]:
                        #print "if"
                        tag_dict[entry[1]][helper_list[0]] = 1
                    else:
                        #print "else"
                        tag_dict[entry[1]][helper_list[0]] += 1

                all_hashtags.append('#' + helper_list[0])
            #else:
                #print "check"
        final_string = ""
        for addtag in all_hashtags:
            final_string += addtag + " "

        if writeIntoDB == "true":
            #print "write in DB"
            cur.execute("UPDATE tweets SET other_tags='" + final_string + "' WHERE id='" + str(entry[0]) + "'")  
    checkAmountOfTweets(conn)
            
'''def checkNullValues(conn):
    cur = conn.cursor()

    cur.execute("SELECT id, Hashtag, Text FROM tweets WHERE other_tags IS NULL")
    
    print cur.fetchall()'''
    
        
def writeInFile():
    print "Write keyword_counter into txt file"
    with io.FileIO("keyword_counter.txt", "w") as file:
        pickle.dump(keyword_counter, file)

def readFromFile():
    with io.FileIO("HashTag_counts.txt", "r") as file:
        my_dict = pickle.load(file)
        #print my_dict["runtastic"]
        
def writeCSV():
    print "Write # counts into csv files"
    #test_dict = {k: v for k, v in tag_dict["gym"].iteritems() if v == 71}
    #print test_dict
    for key in tag_dict:
        with io.FileIO(key + "_tags.csv", "w") as file:
            #fieldnames = ['id', 'value']
            w = csv.writer(file, )
            #w.writerow({'id': 'value'})
            w.writerow(['id', 'value'])
            w.writerows(tag_dict[key].items())

            
def removeLowValues(threshold):
    print "Removing all small values for better visualisation"
    for key in tag_dict:
        tag_dict[key] = {k: v for k, v in tag_dict[key].iteritems() if v >= threshold}
        
def checkDate(entries):
    #print entries[0][3].date()
    dates = {}
    for entry in entries:
        if entry[3].date() not in dates:
            dates[entry[3].date()] = 1
        else:
            dates[entry[3].date()] += 1
    print dates

def checkAmountOfTweets(conn):
    '''if tag_dict: #check if there are values in dict
        for key in tag_dict:
            print key
            cur = conn.cursor()
            cur.execute("SELECT id, Hashtag, Text, created_at FROM tweets")

            entries = cur.fetchall()'''
    cur = conn.cursor()
    for key in tag_dict:
        cur.execute("SELECT id FROM tweets WHERE HashTag = '" + key + "'")
        entries = cur.fetchall()
        #print key, ": ", len(entries), " tweets"
        
        
def clearDB(conn, hashtag):
    cur = conn.cursor()
    cur.execute("DELETE FROM tweets WHERE HashTag = '" + hashtag +"'")
    
def checkOnKeywords(conn):
    for key in keywords:
        for keyword in keywords[key]:
            for tag_key in tag_dict:
                helper_dict = {k: v for k, v in tag_dict[tag_key].iteritems() if keyword in k}
                if tag_key not in keyword_counter:
                    keyword_counter[tag_key] = {}
                if key not in keyword_counter[tag_key]:
                    # check if there are more than one occurences of food in this tweet
                    '''if key == 'food':
                        print helper_dict
                        counts = checkFoodTag(helper_dict)        
                    else:'''
                    keyword_counter[tag_key][key] = len(helper_dict)
                else:
                    #if key == 'food':
                        #print 'ok'''
                    #else:
                    keyword_counter[tag_key][key] += len(helper_dict)
                  
    # just for test output
    for tag_key in keyword_counter:
        for key in keyword_counter[tag_key]:
            print tag_key, ": ", key, ": ", keyword_counter[tag_key][key]
            
    #writeInFile()
'''def checkFoodTag(dict):
    #print dict
    counter = 0
    ret_value = 0
    for helper_key in dict:
        for key in keywords['food']:
            if key in dict[helper_key]:
                counter += 1
        if counter > 1:
            #print counter
            ret_value += counter
            counter = 0
    return ret_value  '''     
            
            
myConnection = MySQLdb.connect( host=hostname, user=username, passwd=password, db=database )
#expandTable("other_tags", myConnection)
#clearDB(myConnection, "workout")
doQuery(myConnection, "false") #set to true, if you want to write into the DB
checkOnKeywords(myConnection)
removeLowValues(50)
writeCSV()

myConnection.close()


