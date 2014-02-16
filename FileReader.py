from pymongo import MongoClient
import subprocess
from datetime import datetime
from PIL import Image
import os
import base64
from threading import Thread
from time import sleep
client = MongoClient()
client = MongoClient('localhost', 27017)

db = client['websites']
collection = db['pages']
__author__ = 'froesler'


def addRecords():
    with open("urls.csv") as f:
        content = f.readlines()

        documentList = []
        for line in content:
            line = line.replace("\n", "")
            parts = line.split(",")
            siteObject = {"_id": parts[0], "url": parts[1]}
            documentList.append(siteObject)


    collection.insert(documentList)
    print("Done adding the documents")


def takeScreenshots():
    threadCount = 7
    print str(collection.find({"image":{"$exists":False}}).count())+" records to go!"
    records = list(collection.find({"image":{"$exists":False}}).limit(30000))

    for i in range(0,threadCount):
        if((i+1)*1000<records.count):
            threadRecords = records[i*1000:(i+1)*1000]
            thread = Thread(target = screenshotThread, args = (threadRecords, ))
            thread.start()
            print "Thread "+ str(i)+" started"
            sleep(3)

def screenshotThread(records):
    for record in records:
        fetchScreenshot(record["_id"], record["url"])

def fetchScreenshot(id, url):
    before = datetime.now()

    try:

        args = "python webkit2png.py -T -o "+id+" http://"+url+""
        print args
        args = args.split()
        subprocess.call(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #
        im = Image.open(id+"-thumb.png")
        width, height = im.size
        box = (0, 0, width, width)
        croppedImage = im.crop(box)
        croppedImage.thumbnail((100, 100))
        croppedImage.save(id+".gif")

        with open(id+".gif", "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read())
            collection.update({"_id": id}, {"$set":{"image":encoded_string}})


        os.remove(id+".gif")
        os.remove(id+"-thumb.png")
        after = datetime.now()
        print("Fetched screenshot for url " + url+" in " + str((after - before).seconds)+" seconds")
    except:
        print "Error at url: " + url



takeScreenshots()
