package xyz.xingcang.elastic_search.Writer;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import xyz.xingcang.elastic_search.bean.Poem;

import java.io.IOException;

/**
 * @author xingcang
 * @create 2020-11-09 4:26 PM
 */
public class writer {
    public static void main(String[] args) throws IOException {
        // 1. get jestClient through Factory
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://localhost:9200")
                .build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        JestClient jestClient = jestClientFactory.getObject();
        // 2. execute
        Poem poem1 = new Poem("短歌行", 9.6);
        Poem poem2 = new Poem("长歌行", 9.5);
        Poem poem3 = new Poem("关雎", 9.7);
        Index index1 = new Index.Builder(poem1)
                .index("movie_test")
                .type("_doc")
                .id("1001")
                .build();

        Index index2 = new Index.Builder(poem2).id("1002").build();
        Index index3 = new Index.Builder(poem3).id("1002").build();
        Bulk bulk = new Bulk.Builder()
                .defaultIndex("movie")
                .defaultType("_doc")
                .addAction(index2)
                .addAction(index3)
                .build();
        jestClient.execute(index1);
        jestClient.execute(bulk);
        jestClient.close();

    }
}
