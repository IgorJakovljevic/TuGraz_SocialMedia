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

    cur.execute("SELECT id, Hashtag, Text FROM tweets")
    #cur.execute("SELECT id, Hashtag, Text FROM tweets WHERE id = 269")
    
    entries = cur.fetchall()

    
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
            if entry[1] != helper_list[0]:
                #print "we are unequal"
                #print current_tag
                #print helper_list[0].lower()
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
            print "write in DB"
            cur.execute("UPDATE tweets SET other_tags='" + final_string + "' WHERE id='" + str(entry[0]) + "'")
        
'''def checkNullValues(conn):
    cur = conn.cursor()

    cur.execute("SELECT id, Hashtag, Text FROM tweets WHERE other_tags IS NULL")
    
    print cur.fetchall()'''
    
        
def writeInFile():
    print "Write # counts into txt file"
    with io.FileIO("HashTag_counts.txt", "w") as file:
        pickle.dump(tag_dict, file)

def readFromFile():
    with io.FileIO("HashTag_counts.txt", "r") as file:
        my_dict = pickle.load(file)
        #print my_dict["runtastic"]

myConnection = MySQLdb.connect( host=hostname, user=username, passwd=password, db=database )
expandTable("other_tags", myConnection)
#for current_tag in crawled_tags:
#tag_dict[current_tag] = {}

doQuery(myConnection, "false") #set to true, if you want to write into the DB

#checkNullValues(myConnection)
writeInFile()
#readFromFile()
myConnection.close()


