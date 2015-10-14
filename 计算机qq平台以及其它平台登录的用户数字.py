# _*_ coding: utf8 _*_

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# link to db
dbClient = MongoClient('mongodb://10.8.8.8:27017')
db = dbClient['matrix-yangcong-prod25']

# open collections
users = db['users']

startDate = datetime.datetime(2015, 9, 1, 0, 0, 0, 000000)
endDate   = datetime.datetime(2015, 9, 3, 0, 0, 0, 000000)


# count qqPlatform users
# user = db.users.aggregate([
#     {"$match": {
#         "usefulData.registDate": {
#             "$gte": ISODate("2015-09-01T00:00:00.000Z"),
#             "$lte": ISODate("2015-09-02T00:00:00.000Z")
#         },
#         "usefulData.q": "qqPlatform"
#     }},
#     {"$group": {
#         "_id": "$_id"
#     }}
# ])
# print(user.result.length)

def calQqPlatformUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                    "$gte": startDate,
                    "$lte": endDate
            },
            "usefulData.q": "qqPlatform"
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

print('user from qq qqPlatform:')
print(len(calQqPlatformUsers(startDate, endDate)))

print('============================')

# 除了QQ外的，其它所有第三方
# user = db.users.aggregate([
#     {"$match": {
#         "usefulData.registDate": {
#             "$gte": ISODate("2015-09-01T00:00:00.000Z"),
#             "$lte": ISODate("2015-09-02T00:00:00.000Z")
#         },
#        "usefulData.q": {
#            "$exists": true,
#            "$ne":"qqPlatform"
#        }
#     }}
# ])
#
# print(user)

def calOtherPlatformUsers(startDate,endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lte": endDate
            },
            "usefulData.q": {
                "$exists": True,
                "$ne":"qqPlatform"
            }
        }}
    ]
    return list(users.aggregate(pipeLine))

print('user from other platform:')
print(len(calOtherPlatformUsers(startDate, endDate)))
