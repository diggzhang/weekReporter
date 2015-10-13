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

#find all teacher id

# Robomongo Script
# var startDate =  ISODate("2015-09-01T00:00:00.000Z")
# var endDate =  ISODate("2015-09-02T00:00:00.000Z")
# db.users.aggregate([
#     {"$match": {
#             "usefulData.registDate": {
#                 "$gte": startDate,
#                 "$lte": endDate
#             },
#             "role": "teacher",
#             "rooms": {"$exists": true, "$not": {"$size": 0}}
#     }},
#     {"$project": {
#             "_id":"$_id",
#             "rooms": "$rooms"
#     }}
# ])

def calTeachersId(startDate, endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lte": endDate
            },
            "role": "teacher",
            "rooms": {"$exists": True, "$not": {"$size": 0}}
        }},
        {"$project": {
            "_id":"$_id",
            "rooms": "$rooms"
        }}
    ]
    return list(users.aggregate(pipeLine))

teacherIdAndRoomId = calTeachersId(startDate, endDate)

# stroe all room id
roomsId = []
for room in teacherIdAndRoomId:
    roomsId.extend(room['rooms'])

#let all rooms id into room collections get all users

# Robomongo Script
# db.rooms.aggregate([
#     {"$match": {
#         "_id": {"$in": allrooms},
#         "users": {"$exists": true, "$not": {"$size": 0}}
#     }},
#     {"$project": {
#         "_id":"$users"
#     }}
# ])

def allStuInRooms(roomsId):
    pipeLine = [
        {"$match": {
            "_id": {"$in": roomsId},
            "users": {"$exists": True, "$not": {"$size": 0}}
        }},
        {"$project": {
            "_id":"$users"
        }}
    ]
    return list(rooms.aggregate(pipeLine))

allStudents = allStuInRooms(roomsId)
# store all studentd id
flatAllstudents = []
for student in allStudents:
    flatAllstudents.extend(student['_id'])
print(len(flatAllstudents))
