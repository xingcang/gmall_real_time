package xyz.xingcang.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis
import xyz.xingcang.bean.StartUpLog
import xyz.xingcang.util.RedisUtil

/**
 * @author xingcang
 * @create 2020-11-05 3:08 PM
 */
object DauHandler {
    def saveMidToRedis(filteredByMidsDStream: DStream[StartUpLog]) = {
        filteredByMidsDStream.foreachRDD(
            rdd => {
                rdd.foreachPartition {
                    iter => {
                        val jedisClient: Jedis = RedisUtil.getJedisClient
                        iter.foreach(
                            log => {
                                jedisClient.sadd(s"DAD:${log.logDate}", log.mid)
                            }
                        )
                        jedisClient.close()
                        }
                }
            }
        )
    }

    def filterByMid(filteredByRedisStartLogDStream: DStream[StartUpLog]) = {
//        filteredByRedisStartLogDStream.map(log => ((log.mid, log.logDate), log))
//            .groupByKey()
//            .flatMap{
//                case ((mid, date), iter) => {
//                    iter.toList.sortWith(_.ts < _.ts).take(1)
//                }
//            }
        filteredByRedisStartLogDStream.map(log => ((log.mid, log.logDate), log))
            .reduceByKey((left, right) => {
                if(left.ts < right.ts) left else right
            })
            .map(_._2)
    }


    private val sdf = new SimpleDateFormat("yyyy-MM-dd")
    def filterByRedis(startLogDStream: DStream[StartUpLog], sc: SparkContext) = {

        startLogDStream.transform(
            rdd => {
                // 1. get connection
                val jedisClient: Jedis = RedisUtil.getJedisClient
                // 2. get mids in Redis
                val mids: util.Set[String] = jedisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}")
                // 3. close the connection
                jedisClient.close()
                // 4. broadcast mids
                val midsBroadCast: Broadcast[util.Set[String]] = sc.broadcast(mids)
                // 5. deduplication using RDD
                rdd.filter(log => !midsBroadCast.value.contains(log.mid))
            }
        )
    }

}
