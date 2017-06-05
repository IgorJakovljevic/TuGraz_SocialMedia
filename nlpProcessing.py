import json
import nltk
import nltk.draw
from nltk.corpus import stopwords
from string import punctuation
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import conll2000
import re
import os
import datetime

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

# Information Extraction Preprocessing
def ie_preprocess(document):
    sentences = nltk.sent_tokenize(document)
    sentences = [nltk.word_tokenize(sent) for sent in sentences]
    sentences = [nltk.pos_tag(sent) for sent in sentences]
    return sentences

def nounPhraseChunking(sentence):
    grammar = r"""
    NP: {<DT|JJ|NN.*>+}          # Chunk sequences of DT, JJ, NN
    PP: {<IN><NP>}               # Chunk prepositions followed by NP
    VP: {<VB.*><NP|PP|CLAUSE>+$} # Chunk verbs and their arguments
    CLAUSE: {<NP><VP>}           # Chunk NP, VP
    """
    cp = nltk.RegexpParser(grammar, loop=2)
    result = cp.parse(sentence)
    return result;

parentPath = "/home/igorjakovljevic/NLPProjects/NLPToolkit/EmailAnalysis/"
inputEmailPath = parentPath + "JSONS/"
output = parentPath + "Processed/"
fileNames = os.listdir(inputEmailPath)
_stopwords = set(stopwords.words('english')+ list(punctuation))



def ProcessEmail(inputEmail, emailName):
    with open(inputEmail) as json_data:
        d = json.load(json_data)
        message = d['message']
        sentences = ie_preprocess(message)
        organisations = []
        persons = []
        dates = []
        urls = GetUrls(message)

        for sent in sentences:
            result = nounPhraseChunking(nltk.ne_chunk(sent))
        
            for subtree in result.subtrees(filter=lambda t: t.label() == 'ORGANIZATION'):
                organisation = " ".join([ w for w,t in subtree.leaves()])
                organisation += " "
                organisations.append(organisation)       
            
            
            for subtree in result.subtrees(filter=lambda t: t.label() == 'PERSON'):
                person = " ".join([ w for w,t in subtree.leaves()])
                persons.append(person)  

        for date in yield_valid_dates(message):
            dates.append(date)

        outputFilePath = output + emailName + "processed.json"
        jsonData = {"urls":urls, "organisations": organisations, "persons" : persons, "dates": dates, "sentences": sentences}
        with open(outputFilePath, 'w') as outfile:
            json.dump(jsonData, outfile, sort_keys = True, indent = 4,ensure_ascii = False)

for fileName in fileNames:
    ProcessEmail(inputEmailPath+fileName,fileName)