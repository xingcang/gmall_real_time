package xyz.xingcang.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
//import org.slf4j.LoggerFactory;
import org.apache.log4j.spi.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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

    @RequestMapping("log")
    public String getLogger(@RequestParam("logString") String logString) {
        log.info(logString);
        return "success";
    }


}
