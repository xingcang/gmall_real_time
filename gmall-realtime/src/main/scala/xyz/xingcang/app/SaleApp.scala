package xyz.xingcang.app

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext, rdd, streaming}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis
import xyz.xingcang.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import xyz.xingcang.gmall_constants.TopicConstants
import xyz.xingcang.util.{JdbcUtil, MyEsUtil, MyKafkaUtil, RedisUtil}

import scala.collection.mutable.ListBuffer
import org.json4s.native.Serialization

import collection.JavaConverters._

/**
 * @author xingcang
 * @create 2020-11-11 08:38
 */
object SaleApp {
    def main(args: Array[String]): Unit = {
        // 1. create stream context
        val sc = new SparkContext(new SparkConf().setAppName("saleApp").setMaster("local[*]"))
        val ssc = new StreamingContext(sc, spark.streaming.Seconds(3))

        // 2. get inputDStream from kafka: OrderInfo and SaleDetail
        val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(TopicConstants.GMALL_ORDER_INFO, ssc)
        val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(TopicConstants.GMALL_ORDER_DETAIL, ssc)


        // 3. transform the orderInfo and saleDetail to case class object

        val idToOrderDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(

            record => {
            val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
            (orderDetail.order_id, orderDetail)
        })

        val idToOrderInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(
            record => {
            val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
            // create_time yyyy-MM-dd HH:mm:ss
            val dateTimeArr: Array[String] = orderInfo.create_time.split(" ")
            orderInfo.create_date = dateTimeArr(0)
            orderInfo.create_hour = dateTimeArr(1).split(":")(0)
            val tel: (String, String) = orderInfo.consignee_tel.splitAt(4)
            orderInfo.consignee_tel = tel._1 + "*******"
            (orderInfo.id, orderInfo)
        })

        // 4. Double-Stream fullOuterJoin using redis cache and save the info to SaleDetail case class
        val fullOuterJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToOrderInfoDStream.fullOuterJoin(idToOrderDetailDStream)

        // 4.1 orderInfo on redis: rowKey: String (orderInfo_id,  orderInfo) + timing   flag r1
        val noUserOrderDetailDStream: DStream[SaleDetail] = fullOuterJoinDStream.mapPartitions(iter => {
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val details = new ListBuffer[SaleDetail]()
            implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
            iter.foreach {
                case (id, (orderOpt: Option[OrderInfo], orderDetailOpt: Option[OrderDetail])) =>
                    val infoRedisKey = s"OrderInfo:$id"
                    val detailRedisKey = s"OrderDetail:$id"
                    if (orderOpt.isDefined) {
                        val orderInfo: OrderInfo = orderOpt.get
                        // 4.1.1  orderInfoDStream  saleDetailDStream  1.id = 2.id
                        if (orderDetailOpt.isDefined) {
                            val orderDetail: OrderDetail = orderDetailOpt.get
                            val saleDetail = new SaleDetail(orderInfo, orderDetail)
                            details += saleDetail
                        }
                        // 4.1.2  put orderInfo_id into r1
                        val infoStr: String = Serialization.write(orderInfo)
                        jedisClient.set(infoRedisKey, infoStr)
                        jedisClient.expire(infoRedisKey, 100)
                        // 4.1.3  get saleDetail in r2
                        if (jedisClient.exists(detailRedisKey)) {
                            val detailJsonSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
                            detailJsonSet.asScala.foreach(detailJson => {
                                val detail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
                                details += new SaleDetail(orderInfo, detail)
                            })
                        }
                    }
                    // 4.2 saleDetail on redis: rowKey: Set ((timestamp + orderInfo_id), saleDetail)  flag r2
                    else {
                        val orderDetail: OrderDetail = orderDetailOpt.get
                        // 4.2.1 get orderInfo in r1
                        if (jedisClient.exists(infoRedisKey)) {
                            val infoStr: String = jedisClient.get(infoRedisKey)
                            details += new SaleDetail(JSON.parseObject(infoStr, classOf[OrderInfo]), orderDetail)
                        } else {
                            // 4.2.2 put detailStr into r2
                            val detailStr: String = Serialization.write(orderDetail)
                            jedisClient.sadd(detailRedisKey, detailStr)
                            jedisClient.expire(detailRedisKey, 100)
                        }
                    }
            }
            jedisClient.close()
            details.iterator
        })

        // 5. merge userInfo with saleDetail
        val saleDetailDStream: DStream[SaleDetail] = noUserOrderDetailDStream.mapPartitions(iter => {
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val connection: Connection = JdbcUtil.getConnection
            val details: Iterator[SaleDetail] = iter.map(
                saleDetail => {
                    val userInfoKey = s"user_info: $saleDetail.id"
                    if (jedisClient.exists(userInfoKey)) {
                        val userInfoStr: String = jedisClient.get(userInfoKey)
                        val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
                        saleDetail.mergeUserInfo(userInfo)
                    } else {
                        val userStr: String = JdbcUtil.getUserInfoFromMysql(
                            connection,
                            "select * from user_info where id = ?",
                            Array(saleDetail.user_id)
                        )
                        val mysqlUserInfo: UserInfo = JSON.parseObject(userStr, classOf[UserInfo])
                        saleDetail.mergeUserInfo(mysqlUserInfo)
                    }
                    saleDetail
                })
            connection.close()
            jedisClient.close()
            details
        })

        // 6. put result into ES
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        saleDetailDStream.foreachRDD(rdd => {
            rdd.foreachPartition(iter => {
                val indexName = s"${TopicConstants.ES_SALE_DETAIL_INDEX_PRE}-${sdf.format(new Date(System.currentTimeMillis()))}"
                val detailIdToSaleDetailInfo: List[(String, SaleDetail)] = iter.toList.map(saleDetail => {
                    (saleDetail.order_detail_id, saleDetail)
                })
                MyEsUtil.insertBulk(indexName,detailIdToSaleDetailInfo)
            })
        })

        // start ssc and block thread
        ssc.start()
        ssc.awaitTermination()
    }
}
