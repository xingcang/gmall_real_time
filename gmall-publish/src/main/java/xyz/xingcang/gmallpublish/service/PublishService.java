package xyz.xingcang.gmallpublish.service;

import java.io.IOException;
import java.util.Map;

/**
 * @author xingcang
 * @create 2020-11-06 9:33 PM
 */
public interface PublishService {
    public Integer getDauTotal(String date);
    public Map getDauTotalHourMap(String date);
    public Double getOrderTotal(String date);
    public Map getOrderTotalHourMap(String date);
    public String getSaleDetail(String date, int startPage, int size, String keyword) throws IOException;
}
