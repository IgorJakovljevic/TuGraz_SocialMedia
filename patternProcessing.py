import re
import json
import datetime
import os

def GetUrls(message):
    return re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', message)
    

def yield_valid_dates(dateStr):
    # will match DD.MM.YYY || DD/MM/YYYY || DD-MM-YYYY
    for match in re.finditer(r"^(?:(?:(?:0?[13578]|1[02])(\/|-|\.)31)\1|(?:(?:0?[1,3-9]|1[0-2])(\/|-|\.)(?:29|30)\2))(?:(?:1[6-9]|[2-9]\d)?\d{2})$|^(?:0?2(\/|-|\.)29\3(?:(?:(?:1[6-9]|[2-9]\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))$|^(?:(?:0?[1-9])|(?:1[0-2]))(\/|-|\.)(?:0?[1-9]|1\d|2[0-8])\4(?:(?:1[6-9]|[2-9]\d)?\d{2})$", dateStr):
        try:
            date = datetime.datetime.strptime(match.group(0), "%m-%d-%Y")
            yield date
            # or you can yield match.group(0) if you just want to
            # yield the date as the string it was found like 05-04-1999
        except ValueError:
            # date couldn't be parsed by datetime... invalid date
            pass

def patternExtractData(inputEmailPath):
    with open(inputEmailPath) as json_data:
        d = json.load(json_data)
        message = d['message']
        urls = GetUrls(message)
        for url in urls:
            print(url)
        print "\n\n"
        for date in yield_valid_dates(message):
            print(date)
        print "\n"


# outputFolder = "/home/igorjakovljevic/NLPProjects/NLPToolkit/EmailAnalysis/JSONS"
# fileNames = os.listdir(outputFolder)

# for fileName in fileNames:
#     patternExtractData(outputFolder+ "/" +fileName)

