package xyz.xingcang.app

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext, streaming}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import redis.clients.jedis.Jedis
import xyz.xingcang.bean.UserInfo
import xyz.xingcang.gmall_constants.TopicConstants
import xyz.xingcang.util.{MyKafkaUtil, RedisUtil}

/**
 * @author xingcang
 * @create 2020-11-11 08:38
 */
object UserInfoAPP {
    def main(args: Array[String]): Unit = {
        // 1. create stream context
        val sc = new SparkContext(new SparkConf().setAppName("SparkTest").setMaster("local[*]"))
        val ssc = new StreamingContext(sc, spark.streaming.Seconds(3))


        // 2. get kafkaDStream of GMALL_USER_INFO
        val userInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(TopicConstants.GMALL_USER_INFO, ssc)

        // 3. put userInfo into Redis
        // rowKey: String (USER_ID: user_id, userInfo)
        //         Hash (user_id, userInfo) 如果数据量较大，则user_id下的数据都会分配给特定的机器，负载不均衡
        userInfoKafkaDStream.foreachRDD(rdd =>
            rdd.foreachPartition(
                iter=> {
                    val jedisClient: Jedis = RedisUtil.getJedisClient
                    iter.foreach(record => {
                        val userInfoJSON: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
                        val userInfoRedisKey: String = s"user_info: $UserInfo.id"
                        jedisClient.set(userInfoRedisKey, userInfoJSON.toString)
                    })
                    jedisClient.close()
            })
        )

        // start ssc and block thread
        ssc.start()
        ssc.awaitTermination()
    }
}
