/* Robomongo脚本 给出指定日期范围  计算得出该时间区域内的所有视频播放次数 uv */

var startDate = ISODate("2015-09-01T00:00:00.000Z");
var endDate   = ISODate("2015-09-02T00:00:00.000Z");

// find unuseful users
var users = db.users.distinct("_id", {"usefulData.registDate": {"$lt": startDate}});

allUserPlayVideoUv = db.points.aggregate([
    {"$match": {
       "_id": {"$nin": users},
       "eventKey": "openVideo",
       "createdBy": {
            "$gte": startDate,
            "$lte": endDate
        }
    }},
    {"$project": {
        "_id": "$_id",
        "user": "$user"
    }},
    {"$group": {
        "_id": "$user"
    }}
]);

print(allUserPlayVideoUv.result.length)
