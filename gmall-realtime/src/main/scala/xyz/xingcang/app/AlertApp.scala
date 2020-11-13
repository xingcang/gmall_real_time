package xyz.xingcang.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import xyz.xingcang.bean.{CouponAlertInfo, EventLog}
import xyz.xingcang.gmall_constants.TopicConstants
import xyz.xingcang.util.{MyEsUtil, MyKafkaUtil}

import scala.util.control.Breaks._

/**
 * @author xingcang
 * @create 2020-11-10 11:17
 */
object AlertApp {


    def main(args: Array[String]): Unit = {

        //需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且过程中没有浏览商品。
        // 达到以上要求则产生一条预警日志。并且同一设备，每分钟只记录一次预警。


        // 1. create stream context
        val sc = new SparkContext(new SparkConf().setAppName("AlterApp").setMaster("local[*]"))
        val ssc = new StreamingContext(sc, spark.streaming.Seconds(3))

        // 2. inputDStream
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(TopicConstants.KAFKA_TOPIC_EVENT, ssc)

        // 3. transform log with case class
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        val midToEventLogDStream: DStream[(String, EventLog)] = kafkaDStream.map(record => {

            val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
            val eventDate: String = sdf.format(new Date(eventLog.ts))
            eventLog.logDate = eventDate.split(" ")(0)
            eventLog.logHour = eventDate.split(" ")(1).split(":")(1)
            (eventLog.mid, eventLog)
        })

        // 4. window 5 min
        val midToEventLogByWindowDStream: DStream[(String, EventLog)] = midToEventLogDStream.window(Minutes(5))

        // 5. groupByKey  get different account
        val boolToCouponAlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToEventLogByWindowDStream.groupByKey().map {
            case (str, logs) =>
                var uids = new java.util.HashSet[String]()
                val itemIds = new java.util.HashSet[String]()
                val events = new java.util.ArrayList[String]()
                var isClick: Boolean = false
                breakable {
                    for (elem <- logs) {
                        val evid: String = elem.evid
                        events.add(evid)
                        if ("clickItem".equals(evid)) {
                            isClick = true
                            break()
                        }
                        else if ("coupon".equals(evid)) {
                            uids.add(elem.mid)
                            itemIds.add(elem.itemid)
                        }
                    }
                }
                (!isClick && uids.size >= 3, CouponAlertInfo(str, uids, itemIds, events, System.currentTimeMillis()))
        }

        // 6. put AlertLog to ES
        val couponAlertInfoDStream: DStream[CouponAlertInfo] = boolToCouponAlertInfoDStream.filter(_._1).map(_._2)

        couponAlertInfoDStream.foreachRDD(rdd => {
            rdd.foreachPartition(iter => {
                val todayStr: Array[String] = sdf.format(new Date(System.currentTimeMillis())).split(" "(0))
                val indexName: String = s"${TopicConstants.ES_ALERT_INDEX_PRE}-$todayStr"

                val docList: List[(String, CouponAlertInfo)] = iter.toList.map(couponAlertInfo => {
                    val date: String = sdf.format(new Date(couponAlertInfo.ts))
                    (s"${couponAlertInfo.mid}-$date", couponAlertInfo)
                })
                MyEsUtil.insertBulk( indexName , docList)
            })
        })

        // start ssc and block thread
        ssc.start()
        ssc.awaitTermination()
    }
}
