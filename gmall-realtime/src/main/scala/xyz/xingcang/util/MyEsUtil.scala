package xyz.xingcang.util

import java.util.{Objects, Properties}

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}
import io.searchbox.client.config.HttpClientConfig

import collection.JavaConverters._

/**
 * @author xingcang
 * @create 2020-11-10 14:13
 */
object MyEsUtil {

    private var factory: JestClientFactory = _
    private var ES_HOST = ""
    private var ES_HTTP_PORT = ""

    private val properties: Properties = PropertiesUtil.load("config.properties")
    ES_HOST = properties.getProperty("ES_HOST")
    ES_HTTP_PORT = properties.getProperty("ES_HTTP_PORT")
    /**
     * 获取客户端
     *
     * @return jestclient
     */
    def getClient: JestClient = {
        if (factory == null) build()
        factory.getObject
    }

    /**
     * 关闭客户端
     */
    def close(client: JestClient): Unit = {
        if (!Objects.isNull(client)) try
            client.shutdownClient()
        catch {
            case e: Exception =>
                e.printStackTrace()
        }
    }

    /**
     * 建立连接
     */
    private def build(): Unit = {
        factory = new JestClientFactory
        factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
            .multiThreaded(true)
            .maxTotalConnection(200) //连接总数
            .connTimeout(10000)
            .readTimeout(10000)
            .build)
    }

    // 批量插入数据到ES
    def insertBulk(indexName: String, docList: List[(String, Any)]): Unit = {

        if (docList.nonEmpty) {

            //获取ES客户端连接
            val jest: JestClient = getClient

            //在循环之前创建Bulk.Builder
            val bulkBuilder: Bulk.Builder = new Bulk.Builder()
                .defaultIndex(indexName)
                .defaultType("_doc")

            //循环创建Index对象,并设置进Bulk.Builder
            for ((id, doc) <- docList) {
                val indexBuilder = new Index.Builder(doc)
                if (id != null) {
                    indexBuilder.id(id)
                }
                val index: Index = indexBuilder.build()
                bulkBuilder.addAction(index)
            }

            //创建Bulk对象
            val bulk: Bulk = bulkBuilder.build()

            var items: java.util.List[BulkResult#BulkResultItem] = null
            try {
                //执行批量写入操作
                items = jest.execute(bulk).getItems

            } catch {
                case ex: Exception => println(ex.toString)
            } finally {
                close(jest)
                println("保存" + items.size() + "条数据")
                for (item <- items.asScala) {
                    if (item.error != null && item.error.nonEmpty) {
                        println(item.error)
                        println(item.errorReason)
                    }
                }
            }
        }
    }
}
