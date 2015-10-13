// 注册漏斗：
// 1.进入首页
// 2.点击注册按钮 clickSignup （注册、立即使用）
// 3.进入注册页面
// 4.点击确认注册按钮
// 4.1 format error  tempSignUp "status": "formatError"
// 4.2 post /user/signup request  tempSignUpPost "status": "sent"
// 5.后端创建用户成功
// 5.1 frontend receive create succeed/failed  tempSignUpPost  "status": "succeed/failed"
// 5.2 frontend send /me request
// 5.3 frontend receive /me request success/failed  tempSignUpGetMe  "status": "succeed/failed"
// 6.成功跳转 autoInlearn/clickInlearn

var out = {
    'enterHome':         {uv: 0, pv: 0},
    'clickSignup':       {uv: 0, pv: 0},
    'enterSignup':       {uv: 0, pv: 0},
    'clickSubmitSignup': {uv: 0, pv: 0},
    'tempSignUp':        {uv: 0, pv: 0},
    'backendSucceed':    {batch: 0, thirdParty: {fywk: 0, twsmcce: 0, qqPlatform: 0}, signup: 0, other: 0},
    'tempSignUpPost':    {uv: 0, pv: 0},
    'tempSignUpGetMe':   {uv: 0, pv: 0},
    'autoInLearnPage':   {uv: 0, pv: 0},
    'clickInLearnPage':  {uv: 0, pv: 0},
    'openVideo':         {uv: 0, pv: 0},
    'finishVideo':       {uv: 0, pv: 0}
}

var startDate = new Date
var endDate = new Date

var startDate = ISODate("2015-09-01T00:00:00.000Z"),
    endDate   = ISODate("2015-09-02T00:00:00.000Z")

var users = db.users.distinct('_id', {'usefulData.registDate': {$lt: startDate}})

db.points.aggregate(
    {$match: {
        createdBy: {$gte: startDate, $lt: endDate},
        user: {$nin: users},
        eventKey: {$in: ['enterHome',
            'clickSignup',
            'enterSignup',
            'clickSubmitSignup',
            'tempSignUp',
            'tempSignUpPost',
            'tempSignUpGetMe',
            'autoInLearnPage',
            'clickInLearnPage',
            'openVideo',
            'finishVideo'
        ]},
    }},
    {$project: {user: 1, eventKey: 1}},
    {$group: { _id: {user: '$user', eventKey: '$eventKey'}, pv: {$sum: 1}}},
    {$project: {user: '$_id.user', eventKey: '$_id.eventKey', pv: 1, _id: 0}}
).forEach(function(item) {
    ++out[item.eventKey].uv
    out[item.eventKey].pv += item.pv
})

out.tempSignUpPost.sent    = {uv: 0, pv: 0},
out.tempSignUpPost.succeed = {uv: 0, pv: 0},
out.tempSignUpPost.failed  = {uv: 0, pv: 0},

out.tempSignUpGetMe.succeed = {uv: 0, pv: 0},
out.tempSignUpGetMe.failed  = {uv: 0, pv: 0},

db.points.aggregate(
    {$match: {
        createdBy: {$gte: startDate, $lt: endDate},
        user: {$nin: users},
        eventKey: {$in: [
            'tempSignUpPost',
            'tempSignUpGetMe',
        ]},
    }},
    {$project: {user: 1, eventKey: 1, status: '$eventValue.status'}},
    {$group: { _id: {user: '$user', eventKey: '$eventKey', status: '$status'}, pv: {$sum: 1}}},
    {$project: {user: '$_id.user', eventKey: '$_id.eventKey', status: '$_id.status', pv: 1, _id: 0}}
).forEach(function(item) {
    ++out[item.eventKey][item.status].uv
    out[item.eventKey][item.status].pv += item.pv
})

db.users.find({
    'privacy': {'$exists': true},
    'usefulData.registDate': {"$exists": true},
    'usefulData.registDate': {$gte: startDate, $lt: endDate},
}).forEach(function(item) {
    if (item.usefulData.q === 'fywk' ||
        item.usefulData.q === 'twsmcce' ||
        item.usefulData.q === 'qqPlatform') {
        ++out.backendSucceed.thirdParty[item.usefulData.q]
        return
    }

    if (item.privacy.username !== item.privacy.useremail &&
        item.privacy.username !== item.privacy.userphone) {
        ++out.backendSucceed.batch
        return
    }

    if (item.privacy.username == item.privacy.useremail ||
        item.privacy.username == item.privacy.userphone) {
        ++out.backendSucceed.signup
        return
    }

    ;++out.backendSucceed.other
})

print('注册漏斗')
printjson(out)
print('')

// 下载漏斗
// 1.进入首页
// 2.点击【下载手机版】按钮（首页第一屏） clickDownloadMobileVersion
// 3.点击下载IOS & Android版 （首页第四屏） downloadios & downloadandroid

var out = {
    'enterHome':                  {uv: 0, pv: 0},
    'clickDownloadMobileVersion': {uv: 0, pv: 0},
    'downloadios':                {uv: 0, pv: 0},
    'downloadandroid':            {uv: 0, pv: 0},
}

db.points.aggregate(
    {$match: {
        createdBy: {$gte: startDate, $lt: endDate},
        user: {$nin: users},
        eventKey: {$in: [
            'enterHome',
            'clickDownloadMobileVersion',
            'downloadios',
            'downloadandroid',
        ]},
    }},
    {$project: {user: 1, eventKey: 1}},
    {$group: { _id: {user: '$user', eventKey: '$eventKey'}, pv: {$sum: 1}}},
    {$project: {user: '$_id.user', eventKey: '$_id.eventKey', pv: 1, _id: 0}}
).forEach(function(item) {
    ++out[item.eventKey].uv
    out[item.eventKey].pv += item.pv
})

print('下载漏斗')
printjson(out)
print('')

// 登陆漏斗
// 1.进入首页 enterHome
// 2.点击登陆按钮 clickLogin
// 3.进入登陆页面
// 4.点击submit登陆按钮  clickSubmitLogin
// 5. UserGuideFrist
// 5.成功登陆  enterChapterList &

var out = {
    'enterHome':        {uv: 0, pv: 0},
    'clickLogin':       {uv: 0, pv: 0},
    'clickSubmitLogin': {uv: 0, pv: 0},
    'enterChapterList': {uv: 0, pv: 0},
}

db.points.aggregate(
    {$match: {
        createdBy: {$gte: startDate, $lt: endDate},
        user: {$nin: users},
        eventKey: {$in: [
            'enterHome',
            'clickLogin',
            'clickSubmitLogin',
            'enterChapterList',
        ]},
    }},
    {$project: {user: 1, eventKey: 1}},
    {$group: { _id: {user: '$user', eventKey: '$eventKey'}, pv: {$sum: 1}}},
    {$project: {user: '$_id.user', eventKey: '$_id.eventKey', pv: 1, _id: 0}}
).forEach(function(item) {
    ++out[item.eventKey].uv
    out[item.eventKey].pv += item.pv
})

print('登陆漏斗')
printjson(out)
print('')

// QQ登陆漏斗
// 1.进入首页
// 2.点击注册按钮
// 3.进入注册页面
// 4.点击QQ账号登陆
// 5.qq登陆成功

var out = {
    'enterHome':        {uv: 0, pv: 0},
    'clickSignup':      {uv: 0, pv: 0},
    'enterSignup':      {uv: 0, pv: 0},
    'clickQQLogin':   {uv: 0, pv: 0},
    'qqLoginSuccess': {uv: 0, pv: 0},
}

db.points.aggregate(
    {$match: {
        createdBy: {$gte: startDate, $lt: endDate},
        user: {$nin: users},
        eventKey: {$in: [
            'enterHome',
            'clickSignup',
            'enterSignup',
            'clickQQLogin',
            'qqLoginSuccess',
        ]},
    }},
    {$project: {user: 1, eventKey: 1}},
    {$group: { _id: {user: '$user', eventKey: '$eventKey'}, pv: {$sum: 1}}},
    {$project: {user: '$_id.user', eventKey: '$_id.eventKey', pv: 1, _id: 0}}
).forEach(function(item) {
    ++out[item.eventKey].uv
    out[item.eventKey].pv += item.pv
})

out.qqLoginSuccess = db.users.count({'usefulData.q': 'qqPlatform', 'usefulData.registDate': {$gte: startDate, $lt: endDate}})

print('QQ登陆漏斗')
printjson(out)
print('')
