package xyz.xingcang.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author xingcang
 * @create 2020-11-05 1:44 PM
 */
object PropertiesUtil {

    def load(propertieName:String): Properties ={
        val prop=new Properties()
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
        prop
    }
}

