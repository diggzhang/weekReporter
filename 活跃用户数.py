# _*_ coding: utf8 _*_

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# link to db
dbClient = MongoClient('mongodb://localhost:27017')
db = dbClient['yangcong-prod25']

# open collections
points = db['points']
users = db['users']

startDate = datetime.datetime(2015, 9, 1, 0, 0, 0, 000000)
endDate   = datetime.datetime(2015, 9, 2, 0, 0, 0, 000000)

output = {}

# find all user from pc
def calPcWork(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "from": "pc"
        }},
        {
            "$group": {
                "_id": "$user"
            }
        }
    ]
    return list(points.aggregate(pipeLine))


def calMobileWork(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "from": "mobile"
        }},
        {
            "$group": {
                "_id": "$user"
            }
        }
    ]
    return list(points.aggregate(pipeLine))

def calIosWork(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "from": "ios"
        }},
        {
            "$group": {
                "_id": "$user"
            }
        }
    ]
    return list(points.aggregate(pipeLine))

def calAndroidWork(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "from": "android"
        }},
        {
            "$group": {
                "_id": "$user"
            }
        }
    ]
    return list(points.aggregate(pipeLine))

print("PC end:")
print len(calPcWork(startDate, endDate));
print("Mobile end:")
print len(calMobileWork(startDate, endDate));
print("iOS end:")
print len(calIosWork(startDate, endDate));
print("Android end:")
print len(calAndroidWork(startDate, endDate));

outArray = []
outArray.extend(calPcWork(startDate, endDate));
outArray.extend(calMobileWork(startDate, endDate));
outArray.extend(calIosWork(startDate, endDate));
outArray.extend(calAndroidWork(startDate, endDate));
print("Total :")
print(len(outArray))

outputlist = []
for x in outArray:
    outputlist.append(x['_id'])

print("Unique this all:")
print(len(set(outputlist)))
