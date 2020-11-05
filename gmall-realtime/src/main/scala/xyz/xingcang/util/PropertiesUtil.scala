package xyz.xingcang.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * @author xingcang
 * @create 2020-11-05 1:44 PM
 */
object PropertiesUtil {
    def load(propertiesName: String) = {
        val properties = new Properties()
        properties.load(new InputStreamReader(
            Thread.currentThread().getContextClassLoader.getResourceAsStream(
                "propertiesName"), StandardCharsets.UTF_8))
        properties
    }
}
