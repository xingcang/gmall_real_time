package xyz.xingcang.gmalllogger.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
//import org.slf4j.LoggerFactory;
import org.apache.log4j.spi.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import xyz.xingcang.gmall_constants.TopicConstants;

/**
 * @author xingcang
 * @create 2020-11-03 7:33 PM
 */

@RestController
@Slf4j
public class LoggerController {


//    LoggerFactory
    @RequestMapping("test01")
    public String test01() {
        System.out.println("1111");
        return "success";
    }

    public String test02(@RequestParam("name") String name,
                         @RequestParam("age") int age) {
        System.out.println(name + ": " + age);
        return "success";
    }

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;


    @RequestMapping("log")
    public String getLogger(@RequestParam("logString") String logString) {
        JSONObject jsonObject = JSONObject.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());
        String jsonStr = jsonObject.toString();
        log.info(jsonStr);
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(TopicConstants.KAFKA_TOPIC_STARTUP, jsonStr);
        } else kafkaTemplate.send(TopicConstants.KAFKA_TOPIC_EVENT, jsonStr);
        return "success";
    }
}
