package xyz.xingcang.app

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext, rdd, streaming}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import xyz.xingcang.bean.OrderInfo
import xyz.xingcang.gmall_constants.TopicConstants
import xyz.xingcang.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.phoenix.spark._

/**
 * @author xingcang
 * @create 2020-11-10 18:25
 */
object GmvApp {



    def main(args: Array[String]): Unit = {
        // 1. create stream context
        val sc = new SparkContext(new SparkConf().setAppName("SparkTest").setMaster("local[*]"))
        val ssc = new StreamingContext(sc, spark.streaming.Seconds(3))

        val properties: Properties = PropertiesUtil.load("config.properties")
        val kafkaInputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(TopicConstants.GMALL_ORDER_INFO, ssc)

        val orderInfoDStream: DStream[OrderInfo] = kafkaInputDStream.map(record => {

            val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
            val orderDateArr: Array[String] = orderInfo.create_time.split(" ")
            orderInfo.create_date = orderDateArr(0)
            orderInfo.create_hour = orderDateArr(1).split(":")(0)
            val telephoneAct: (String, String) = orderInfo.consignee_tel.splitAt(4)
            orderInfo.consignee_tel = telephoneAct._1 + "*******"
            orderInfo
        })
        orderInfoDStream.foreachRDD(rdd=>{
            rdd.saveToPhoenix(TopicConstants.GMALL_ORDER_INFO,
                classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()), // 通过反射得到表的列名
                HBaseConfiguration.create(),
                Some(properties.getProperty("zookeeper.servers.list") + ":" + properties.getProperty("zookeeper.server.port"))
            )
        })

        // start ssc and block thread
        ssc.start()
        ssc.awaitTermination()


    }
}
