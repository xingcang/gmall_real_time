package xyz.xingcang.gmallpublish.controller;

import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import xyz.xingcang.gmallpublish.service.PublishService;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xingcang
 * @create 2020-11-06 9:28 PM
 */
@RestController
public class PublisherController {

    @Autowired
    PublishService publishService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {
        ArrayList<Map> result = new ArrayList<>();

        Integer dauTotal = publishService.getDauTotal(date);
        Double orderTotal = publishService.getOrderTotal(date);
        // 新增日活跃
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);
        // 新增设备
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);
        // 新增交易额
        HashMap<String, Object> gvmMap = new HashMap<>();
        gvmMap.put("id", "order_amount");
        gvmMap.put("name", "新增交易额");
        gvmMap.put("value", orderTotal);
        // 结果放入list
        result.add(dauMap);
        result.add(newMidMap);
        result.add(gvmMap);
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,
                                     @RequestParam("date") String date) {

        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        Map todayMap = null;
        Map yesterdayMap = null;
        HashMap<String, Map> result = new HashMap<>();
        if("dau".equals(id)) {
            // get dauMap
            todayMap = publishService.getDauTotalHourMap(date);
            yesterdayMap = publishService.getDauTotalHourMap(yesterday);
        } else {
            todayMap = publishService.getOrderTotalHourMap(date);
            yesterdayMap = publishService.getOrderTotalHourMap(date);
        }
        // put result to list
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);
        return JSONObject.toJSONString(result);
    }

}

