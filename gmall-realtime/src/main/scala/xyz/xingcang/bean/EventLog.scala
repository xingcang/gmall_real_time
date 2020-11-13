package xyz.xingcang.bean

/**
 * @author xingcang
 * @create 2020-11-10 11:30
 */
case class EventLog(mid: String,
                    uid: String,
                    appid: String,
                    area: String,
                    os: String,
                    `type`: String,
                    evid: String,
                    pgid: String,
                    npgid: String,
                    itemid: String,
                    var logDate: String,
                    var logHour: String,
                    var ts: Long)
