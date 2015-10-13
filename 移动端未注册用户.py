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
rooms = db['rooms']

# configure daterange
startDate = datetime.datetime(2015, 9, 1, 0, 0, 0, 000000)
endDate   = datetime.datetime(2015, 9, 2, 0, 0, 0, 000000)

# find all ios user in points
def iosUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "from": "ios"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

print("ios users:")
print(len(iosUsers(startDate, endDate)))

# find all android user in points
def androidUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "from": "android"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

print("android users:")
print(len(androidUsers(startDate, endDate)))

# unique all ios android get a all number
alliosUsers = iosUsers(startDate, endDate)
allandroidUsers = androidUsers(startDate, endDate)
allUserNumber = len(alliosUsers) + len(allandroidUsers)
print("All Users Number:")
print(allUserNumber)

allUsers = []
for ios in alliosUsers:
    allUsers.append(ios['_id'])

for andoird in allandroidUsers:
    allUsers.append(andoird['_id'])

allUsersId = allUsers

# all user into users collections $in  get a numner
def findRegUser(allUsersId):
    pipeLine = [
        {"$match": {
            "_id": {"$in": allUsersId}
        }},
        {"$project": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

regUsers = findRegUser(allUsersId)
print("reg users:")
print(len(regUsers))
regUserNumber = len(regUsers)

# allnumber - a number = not regist

print("not reg users:")
print(allUserNumber - regUserNumber)
