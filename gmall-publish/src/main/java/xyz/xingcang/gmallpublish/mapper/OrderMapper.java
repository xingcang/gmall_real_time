package xyz.xingcang.gmallpublish.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author xingcang
 * @create 2020-11-07 10:42 AM
 */
public interface OrderMapper {

    public Double selectOrderAmountTotal(String date);
    public List<Map> selectOrderAmountHourMap(String date);

}
