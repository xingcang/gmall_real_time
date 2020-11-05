package xyz.xingcang.app

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext, streaming}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import xyz.xingcang.bean.StartUpLog
import xyz.xingcang.gmall_constants.TopicConstants
import xyz.xingcang.handler.DauHandler
import xyz.xingcang.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.phoenix.spark._

/**
 * @author xingcang
 * @create 2020-11-05 2:19 PM
 */
object DauApp {
    def main(args: Array[String]): Unit = {
        // 1. create stream context
        val sc = new SparkContext(new SparkConf().setAppName("SparkTest").setMaster("local[*]"))
        val ssc = new StreamingContext(sc, spark.streaming.Seconds(3))
        val properties: Properties = PropertiesUtil.load("config.properties")

        // 2. get kafka Stream
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(TopicConstants.KAFKA_TOPIC_STARTUP, ssc)

        // 3. transform the record to case class with date field
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
        val startLogDStream: DStream[StartUpLog] = kafkaDStream.map(
            record => {
                val startUpLogInfo: String = record.value()
                val startJsonLog: StartUpLog = JSON.parseObject(startUpLogInfo, classOf[StartUpLog])
                val logDate: String = sdf.format(new Date(startJsonLog.ts))
                val logDateInfo: Array[String] = logDate.split(" ")
                startJsonLog.logDate = logDateInfo(0)
                startJsonLog.logHour = logDateInfo(1)
                startJsonLog
            }
        )

        // 4. deduplication across batches with Redis
        val filteredByRedisStartLogDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startLogDStream, ssc.sparkContext)

        // 5. deduplication in the same batch
        val filteredByMidsDStream: DStream[StartUpLog] = DauHandler.filterByMid(filteredByRedisStartLogDStream)

        // 6. save the distinct mid to redis
        DauHandler.saveMidToRedis(filteredByMidsDStream)

        // 7. save log to phoenix
        filteredByMidsDStream.foreachRDD(
            rdd => {
                rdd.saveToPhoenix("GMALL200621",
                    Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
                    HBaseConfiguration.create(),
                    Some(properties.getProperty("zookeeper.servers.list") + ":" + properties.getProperty("zookeeper.server.port"))
                )
            }
        )

        // start ssc and block thread
        ssc.start()
        ssc.awaitTermination()
    }
}
