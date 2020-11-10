package xyz.xingcang.gmallpublish.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author xingcang
 * @create 2020-11-06 9:29 PM
 */

public interface DauMapper {
    public Integer selectDauTotal(String date);
    public List<Map> selectDauTotalHourMap(String date);
}
