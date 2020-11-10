package xyz.xingcang.gmallpublish.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import xyz.xingcang.gmallpublish.mapper.DauMapper;
import xyz.xingcang.gmallpublish.mapper.OrderMapper;
import xyz.xingcang.gmallpublish.service.PublishService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xingcang
 * @create 2020-11-06 9:31 PM
 */

@Service
public class PublishServiceImpl implements PublishService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {
        List<Map> list = dauMapper.selectDauTotalHourMap(date);
        HashMap<String, Long> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return result;
    }

    @Override
    public Double getOrderTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderTotalHourMap(String date) {
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        HashMap<String, Double> result = new HashMap<>();
        for (Map map : list) {
            result.put( (String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }
}