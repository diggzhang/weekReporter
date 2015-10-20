# _*_ coding: utf8 _*_

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# link to db
dbClient = MongoClient('mongodb://localhost:27017')
db = dbClient['yangcong-prod25']

# open collections
points = db['points']

startDate = datetime.datetime(2015, 10, 9)
endDate   = datetime.datetime(2015, 10, 15)

# all enterHome

def enterHomeUserId(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "eventKey": "enterHome"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allEnterHomeUserId = enterHomeUserId(startDate, endDate)

# processing allEnterHomeUserId as a array
enterHomeUserIdArray = []
for x in allEnterHomeUserId:
    enterHomeUserIdArray.append(x['_id'])
print("EnterHome:")
print(len(enterHomeUserIdArray))


# clickSignup

def clickSignupUserId(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "user": {"$in": userId},
            "eventKey": "clickSignup"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allClickSignupUserId = clickSignupUserId(startDate, endDate, enterHomeUserIdArray)

clickSignupUserIdArray = []
for x in allClickSignupUserId:
    clickSignupUserIdArray.append(x['_id'])
print("clickSignup: ")
print(len(clickSignupUserIdArray))

# enterSignup

def enterSignupUserId(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "user": {"$in": userId},
            "eventKey": "enterSignup"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allEnterSignupUserId = enterSignupUserId(startDate, endDate, clickSignupUserIdArray)

enterSignupUserIdArray = []
for x in allEnterSignupUserId:
    enterSignupUserIdArray.append(x['_id'])
print("enterSignup: ")
print(len(enterSignupUserIdArray))

# clickSubmitSignup

def clickSubmitSignup(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "user": {"$in": userId},
            "eventKey": "clickSubmitSignup"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allClickSubmitSignupUserId = clickSubmitSignup(startDate, endDate, enterSignupUserIdArray)

clickSubmitSignupUserIdArray = []
for x in allClickSubmitSignupUserId:
    clickSubmitSignupUserIdArray.append(x['_id'])
print("clickSubmitSignup: ")
print(len(clickSubmitSignupUserIdArray))

# tempSignUpGetMe

def tempSignUpGetMe(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "user": {"$in": userId},
            "eventKey": "tempSignUpGetMe"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))
alltempSignUpGetMeUserId = tempSignUpGetMe(startDate, endDate, clickSubmitSignupUserIdArray)

tempSignUpGetMeArray = []
for x in alltempSignUpGetMeUserId:
    tempSignUpGetMeArray.append(x['_id'])
print("tempSignUpGetMe: ")
print(len(tempSignUpGetMeArray))

# openVideo

def openVideo(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "user": {"$in": userId},
            "eventKey": "openVideo"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allopenVideoUserId = openVideo(startDate, endDate, tempSignUpGetMeArray)

openVideoArray = []
for x in allopenVideoUserId:
    openVideoArray.append(x['_id'])
print("openVideo: ")
print(len(openVideoArray))

# finishVideo

def finishVideo(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "user": {"$in": userId},
            "eventKey": "finishVideo"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allfinishVideoUserId = finishVideo(startDate, endDate, openVideoArray)

finishVideoArray = []
for x in allfinishVideoUserId:
    finishVideoArray.append(x['_id'])
print("finishVideo: ")
print(len(finishVideoArray))
