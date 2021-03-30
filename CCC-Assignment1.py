#-*- coding:utf-8 -*-
from emoji import UNICODE_EMOJI
from functools import reduce
from mpi4py import MPI
import time
import json
import sys
import re
import os

def merge(left, right):
    i = 0
    j = 0
    sorted_list = []

    while(i<len(left)) & (j<len(right)):
        if left[i][1] > right[j][1]:
            sorted_list.append(left[i])
            i = i+1
        else:
            sorted_list.append(right[j])
            j = j+1
    sorted_list = sorted_list + right[j:] + left[i:]
    return sorted_list


class Node(object):
    def __init__(self, key, data=None):
        self.key = key
        self.data = data
        self.succ = {}

class Trie(object):
    def __init__(self):
        self.head = Node(None)

    def insert(self, string):
        curr_node = self.head

        for char in string:
            if char not in curr_node.succ:
                curr_node.succ[char] = Node(char)

            curr_node = curr_node.succ[char]
        curr_node.data = string

    def search(self, string):
        curr_node = self.head

        for char in string:
            if char in curr_node.succ:
                curr_node = curr_node.succ[char]
            else:
                return False

        if (curr_node.data != None):
            return True


dataFilePath = os.getcwd() + "/" + sys.argv[1]
condition = sys.argv[2]


pPattern = re.compile(r"[!\"$%&\'()*+,-.\/:;<=>?@^\[\]\\ \n]+")
pattern = re.compile(r"([^A-Za-z0-9_]{1}|[0-9_]*[A-Za-z]+[\w]*)")


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


if rank == 0:
    start = time.time()
    startTime = time.strftime('%c', time.localtime(time.time()))
    print("\n\n" + "Program start time : " + str(startTime) + "\n\n")


trieH = Trie()
trieL = Trie()
trieEmoji = Trie()


filteredHashtags = [{} for _ in range(size)]
filteredLanguages = [{} for _ in range(size)]


gatheredHashtags = []
gatheredLanguages = []


tagCount = {}
codeCount = {}


sortedHashtags = [{} for _ in range(size)]
sortedLanguages = [{} for _ in range(size)]


for emoji in UNICODE_EMOJI.keys():
    trieEmoji.insert(repr(emoji))


f = open(dataFilePath, "r") #localmachine : //


for lineNumber, line in enumerate(f):
    if (lineNumber%size) == rank:
        texts = []
        langs = []

        try:
            json_str = json.loads(line[:-2]) #localmachine : -3, spartan : -2

            text = json_str['doc']['text']
            texts.append(text)
            lang = json_str['doc']['metadata']['iso_language_code']
            langs.append(lang)

            try:
                rt_text = json_str['doc']['retweeted_status']['text']
                texts.append(rt_text)
                rt_lang = json_str['doc']['retweeted_status']['metadata']['iso_language_code']
                langs.append(rt_lang)
            except:
                pass

            try:
                quoted_text = json_str['doc']["quoted_status"]["text"]
                texts.append(quoted_text)
                quoted_lang = json_str['doc']["quoted_status"]["metadata"]["iso_language_code"]
                langs.append(quoted_lang)
            except:
                pass

            for text in texts:
                splitTweet = pPattern.sub(" ", text).split(" ")
                for word in splitTweet:
                    if word.startswith("#") and len(word) > 1:
                        if re.search("^#{1,}.*#{1}", word) != None:
                            word = re.sub("^#{1,}.*#{1}", "#", word, 1)
                        if re.search(u'\u2026', word) != None:
                            word = re.sub(u'\u2026', '', word)

                        subMatches = pattern.findall(word[1:])
                        count = 1

                        if len(subMatches) > 0:
                            for subMatch in subMatches:
                                if trieEmoji.search(repr(subMatch)) == True:
                                    break
                                else:
                                    count = count + len(subMatch)

                            if count > 1:
                                hashtag = word[:count].upper()
                                strHashtag = repr(hashtag)

                                partition = 0
                                try:
                                    hData = bytearray(strHashtag.encode('utf-8'))
                                    for h in hData:
                                        partition = partition + h
                                    partition = partition % size

                                except:
                                    partition = 0
                                    pass

                                if trieH.search(strHashtag) == True:
                                    filteredHashtags[partition][strHashtag] = filteredHashtags[partition][strHashtag] + 1
                                else:
                                    filteredHashtags[partition][strHashtag] = 1
                                    trieH.insert(strHashtag)

            for code in langs:
                try:
                    partition = 0
                    hData = bytearray(str(code).encode('UTF-8'))
                    for h in hData:
                        partition = partition + h
                    partition = partition % size

                except:
                    partition = size - 1
                    pass

                if trieL.search(code) == True:
                    filteredLanguages[partition][code] = filteredLanguages[partition][code] + 1
                else:
                    filteredLanguages[partition][code] = 1
                    trieL.insert(code)

        except:
            #if rank read incomplete json object
            pass

#each processor finishes reading file and filtering hashtags and language codes
    else:
        pass


f.close()


gatheredHashtags = comm.alltoall(filteredHashtags)
gatheredLanguages = comm.alltoall(filteredLanguages)


for hashtags in gatheredHashtags:
    for tag in hashtags:
        if tag in tagCount:
            tagCount[tag].append(hashtags[tag])
        else:
            tagCount[tag] = [hashtags[tag]]

for languages in gatheredLanguages:
    for code in languages:
        if code in codeCount:
            codeCount[code].append(languages[code])
        else:
            codeCount[code] = [languages[code]]


for tag in tagCount:
    tagCount[tag] = reduce((lambda x, y: x + y), tagCount[tag])

for code in codeCount:
    codeCount[code] = reduce((lambda x, y: x + y), codeCount[code])


tagCount = sorted(tagCount.items(), key=lambda x: x[1], reverse=True)
codeCount = sorted(codeCount.items(), key=lambda x: x[1], reverse=True)


sortedHashtags = comm.gather(tagCount, root=0)
sortedLanguages = comm.gather(codeCount, root=0)


if rank == 0:
    mostHashtags = []
    mostLanguages = []


    if size > 1 :
        for iter in range(1, size):
            sortedHashtags[0] = merge(sortedHashtags[0], sortedHashtags[iter])
            sortedLanguages[0] = merge(sortedLanguages[0], sortedLanguages[iter])


    mostHashtags = sortedHashtags[0][:10]
    mostLanguages = sortedLanguages[0][:10]


    print("[ Condition ] : " + condition + "\n")
    print("[ Data ] : " + dataFilePath + "\n")
    print("[ Number of Processors ] : " + str(size) + "\n\n")


    print("[ Most 10 Hashtags ]")
    for i in range(10):
        print(i+1,mostHashtags[i][0],"\t",mostHashtags[i][1])


    print("\n\n\n[ Most 10 Languages ]")
    for i in range(10):
        print(i+1,mostLanguages[i][0],"\t",mostLanguages[i][1])


    finishTime = time.strftime('%c', time.localtime(time.time()))
    stop = time.time()

    print("\n\n" + "Program finish time : " + str(finishTime))

    print("\n" + "Elapsed time(Seconds) : " + str(stop - start) + "\n\n")