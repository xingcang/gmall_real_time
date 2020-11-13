package xyz.xingcang.bean

/**
 * @author xingcang
 * @create 2020-11-10 11:57
 */
case class CouponAlertInfo(mid: String,
                           uids: java.util.HashSet[String],
                           itemIds: java.util.HashSet[String],
                           events: java.util.List[String],
                           ts: Long)
