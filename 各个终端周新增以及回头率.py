# _*_ coding: utf8 _*_

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# link to db
# dbClient = MongoClient('mongodb://localhost:27017')
# db = dbClient['yangcong-prod25']
dbClient = MongoClient('mongodb://10.8.8.8:27017')
db = dbClient['matrix-yangcong-prod25']

# open collections
points = db['points']
users = db['users']

startDate = datetime.datetime(2015,9,1)

def calNewAndRet(startDate):
    # this week's new users
    # this week's retation users
    # the unactivy users
    # date now
    weekNew = 0
    weekRet = 0
    unActList = []
    dateNow = 0

    # while for count every day
    while dateNow < 7:
        # this day's init and end time
        initDate = startDate + datetime.timedelta(days=dateNow)
        endDate = initDate + datetime.timedelta(days = 1)

        # find the startDate's first doc as start doc
        # than get the start doc's id
        # as so as endDate's first doc
        startDoc = points.find_one({"createdBy": {"$gte": initDate}})
        startId = startDoc['_id']

        endDoc = points.find_one({"createdBy": {"$gte": endDate}})
        endId = endDoc['_id']

        pipeLine = [
            {"$match": {
                "_id": {
                    "$gte": startId,
                    "$lt": endId
                },
                # "from": "pc"
                # "from": "android"
                # "from": "ios"
            }},
            {"$group": {
                "_id": "$user"
            }}
        ]

        # get the id
        activityUsers = list(points.aggregate(pipeLine))
        activityUsers = [d['_id'] for d in activityUsers]

        # cal retation
        activityUsers = set(activityUsers)
        retUsers = activityUsers.intersection(set(unActList))

        weekRet += len(retUsers)
        unActList = set(unActList).difference(retUsers)

        # Add new daily users
        dayNewUsers = []
        query = users.find({"usefulData.registDate": {"$gte": initDate, "$lt": endDate}})

        for doc in query:
            dayNewUsers.append(doc['_id'])

        dayNewUsers = set(dayNewUsers)

        weekNew += len(dayNewUsers)
        unActList = unActList.union(dayNewUsers)

        print(str(initDate))
        print 'New: ' + str(weekNew) + ' | ' + 'Ret: ' + str(weekRet)

        dateNow += 1

    print '==============='
    print 'week New user:'
    print weekNew
    print 'week Ret:'
    print weekRet

calNewAndRet(startDate)
