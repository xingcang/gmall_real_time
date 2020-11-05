package xyz.xingcang.bean

/**
 * @author xingcang
 * @create 2020-11-05 2:15 PM
 */

case class StartUpLog(
                     mid:String,
                     uid:String,
                     appid:String,
                     area:String,
                     os:String,
                     ch:String,
                     `type`:String,
                     vs:String,
                     var logDate:String,
                     var logHour:String,
                     var ts:Long
                     )
