package xyz.xingcang.gmallpublish.service.impl;


import com.alibaba.fastjson.JSON;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import xyz.xingcang.gmallpublish.bean.Option;
import xyz.xingcang.gmallpublish.bean.Stat;
import xyz.xingcang.gmallpublish.mapper.DauMapper;
import xyz.xingcang.gmallpublish.mapper.OrderMapper;
import xyz.xingcang.gmallpublish.service.PublishService;

import java.io.IOException;
import java.util.ArrayList;
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

    @Autowired
    private JestClient jestClient;

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
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

    @Override
    public String getSaleDetail(String date, int startPage, int size, String keyword) throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // bool query
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword));
        searchSourceBuilder.query(boolQueryBuilder);

        // aggregation by age and gender
        TermsAggregationBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        TermsAggregationBuilder ageAggs = AggregationBuilders.terms("groupby_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // page from ... and size
        searchSourceBuilder.from((startPage - 1) * size);
        searchSourceBuilder.size(size);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("gmall2020_sale_detail-query")
                .addType("_doc")
                .build();

        SearchResult searchResult = jestClient.execute(search);

        HashMap<String, Object> result = new HashMap<>();

        Long total = searchResult.getTotal();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        ArrayList<Map> details = new ArrayList<>();
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }

        // gender
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");
        Long maleCount = 0L;
        for (TermsAggregation.Entry entry : groupby_user_gender.getBuckets()) {
            if (entry.getKey().equals("M")) {
                maleCount = entry.getCount();
            }
        }
        double maleRatio = Math.round(maleCount * 1000 / total) / 10D;
        double femaleRadio = Math.round((100D - maleRatio) * 10D) / 10D;

        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRadio);
        ArrayList<Option> genderOptions = new ArrayList<>();
        genderOptions.add(maleOpt);
        genderOptions.add(femaleOpt);

        Stat genderStat = new Stat("用户性别占比", genderOptions);

        // age
        TermsAggregation groupby_user_age = aggregations.getTermsAggregation("groupby_user_age");
        Long lower20 = 0L;
        Long upper20To30 = 0L;
        Long upper30 = 0L;
        for (TermsAggregation.Entry entry : groupby_user_age.getBuckets()) {
            if (Integer.parseInt(entry.getKey()) < 20) {
                lower20 = entry.getCount();
            } else if (Integer.parseInt(entry.getKey() )>= 30) {
                upper30 = entry.getCount();
            }
        }
        Double lower20Ratio = Math.round(lower20 * 1000D / total) / 10D;
        Double upper30Ratio = Math.round(upper30 * 1000D / total) / 10D;
        Double upper20to30 = Math.round((100D - lower20Ratio - upper30Ratio) * 10D) / 10D;

        Option lower20Opt = new Option("20岁以下", lower20Ratio);
        Option upper20to30Opt = new Option("20岁到30岁", upper20to30);
        Option upper30RatioOpt = new Option("30岁及30岁以上", upper30Ratio);

        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(lower20Opt);
        ageOptions.add(upper20to30Opt);
        ageOptions.add(upper30RatioOpt);

        Stat ageStat = new Stat("用户年龄占比", ageOptions);
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", details);

        return JSON.toJSONString(result);
    }
}