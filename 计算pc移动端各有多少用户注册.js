/* Robomongo脚本 给出指定日期范围  计算得出该时间区域内的所有用户 */

var startDate = ISODate("2015-09-01T00:00:00.000Z");
var endDate   = ISODate("2015-09-02T00:00:00.000Z");

// find unuseful users
var users = db.users.distinct("_id", {"usefulData.registDate": {"$lt": startDate}});

allUser = db.users.aggregate([
    {"$match": {
       "_id": {"$nin": users},
        "usefulData.registDate": {
            "$gte": startDate,
            "$lte": endDate
        }
    }},
    {"$match": {
      "usefulData.from": "signup"   //android ios
    }},
    {"$project": {
        "_id": "$_id"
    }}
]);

print(allUser.result.length)
