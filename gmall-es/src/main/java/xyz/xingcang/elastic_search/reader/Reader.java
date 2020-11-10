package xyz.xingcang.elastic_search.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.MinAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * @author xingcang
 * @create 2020-11-09 6:15 PM
 */
public class Reader {
    public static void main(String[] args) throws IOException {
        // 1. get client
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://localhost:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        JestClient jestClient = jestClientFactory.getObject();

        // 2. create query

        // 2.1  source builder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 2.2  aggregation
        MinAggregationBuilder minAgeGroup = AggregationBuilders.min("minAge").field("age");
        searchSourceBuilder.aggregation(minAgeGroup);

        TermsAggregationBuilder countOfGender = AggregationBuilders.terms("countOfGender").field("gender");
        searchSourceBuilder.aggregation(countOfGender);

        // 2.3 query-boolean  是一个层层嵌套的查询语句, 对象包方法/属性, 方法/属性包对象!
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder class_id = new TermQueryBuilder("class_id", "0621");
        boolQueryBuilder.filter(class_id);

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo2", "球");
        boolQueryBuilder.must(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        // 2.4 分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);

        // 3. add the search query, Index and type
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student")
                .addType("_doc")
                .build();

        // 4. parse result
        SearchResult searchResult = jestClient.execute(search);
        Long total = searchResult.getTotal();
//        System.out.println("命中: " + total);
        // 4.1 aggregation
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation countOfGender1 = aggregations.getTermsAggregation("countOfGender");
        System.out.println(countOfGender1);
        MinAggregation minAge = aggregations.getMinAggregation("minAge");
        System.out.println(minAge);
        // 4.2 query
        for (SearchResult.Hit<Map, Void> hit : searchResult.getHits(Map.class)) {
            for (Object o : hit.source.entrySet()) {
                System.out.println(o.toString());
            }
        }
        jestClient.close();
    }
}
