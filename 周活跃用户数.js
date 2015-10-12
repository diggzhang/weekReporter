/* Robomongo脚本 给出指定日期范围  计算得出该时间区域内的活跃用户 */

var startDate = ISODate("2015-09-01T00:00:00.000Z");
var endDate   = ISODate("2015-09-02T00:00:00.000Z");

// find unuseful users

userIsWork = db.points.aggregate([
    {"$match": {
        "from": "ios", //pc android
        "createdBy": {
            "$gte": startDate,
            "$lte": endDate
        }
    }},
    {"$group": {
        "_id": "$user"
    }}
])

print(userIsWork.result.length)
