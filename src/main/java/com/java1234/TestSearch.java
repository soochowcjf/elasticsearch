package com.java1234;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

/**
 * author:chenjinfeng
 * Date:2018/5/27
 * Time:18:11
 * 测试es的索引
 */
public class TestSearch {
    //服务器地址
    private static String host = "192.168.25.131";
    //端口号
    private static int port = 9300;
    //集群名称
    public static final String CLUSTER_NAME = "my-application";

    private static Settings.Builder settings = Settings.builder().put("cluster.name",CLUSTER_NAME);

    private TransportClient client=null;

    /**
     * 得到es客户端
     * @throws Exception
     */
    @Before
    public void getClient() throws Exception {
        client = new PreBuiltTransportClient(settings.build())
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(TestSearch.host), TestSearch.port));
        System.out.println(client);
    }

    /**
     * 关闭连接
     */
    @After
    public void close(){
        if(client!=null){
            client.close();
        }
    }

    /**
     * es查询所有
     * @throws Exception
     */
    @Test
    public void searchAll() throws Exception {
        //得到查询请求构造
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("film").setTypes("dongzuo");
        //得到查询结果，查询所有
        SearchResponse searchResponse = searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery())
                                                            .execute()
                                                            .actionGet();
        //得到查询hits
        SearchHits hits = searchResponse.getHits();
        //遍历hits
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
    /**
     * es分页查询
     * @throws Exception
     */
    @Test
    public void searchPages() throws Exception {
        //得到查询请求构造
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("film").setTypes("dongzuo");
        //得到查询结果,设置分页条件
        SearchResponse searchResponse = searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery())
                .setFrom(1)
                .setSize(3)
                .execute()
                .actionGet();
        //得到查询hits
        SearchHits hits = searchResponse.getHits();
        //遍历hits
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
    /**
     * es排序查询
     * @throws Exception
     */
    @Test
    public void searchSort() throws Exception {
        //得到查询请求构造
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("film").setTypes("dongzuo");
        //得到查询结果,设置排序规则
        SearchResponse searchResponse = searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery())
                .addSort("publishDate",SortOrder.DESC)
                .execute()
                .actionGet();
        //得到查询hits
        SearchHits hits = searchResponse.getHits();
        //遍历hits
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
    /**
     * es数据列的过滤
     * @throws Exception
     */
    @Test
    public void searchInclude() throws Exception {
        //得到查询请求构造
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("film").setTypes("dongzuo");
        //得到查询结果,设置需要查询的列，或者不需要查询的列
        SearchResponse searchResponse = searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery())
                .setFetchSource(new String[]{"title","price"},null)
                .execute()
                .actionGet();
        //得到查询hits
        SearchHits hits = searchResponse.getHits();
        //遍历hits
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * es条件查询结果高亮显示
     * @throws Exception
     */
    @Test
    public void searchHighlight() throws Exception {
        //得到查询请求构造
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("film").setTypes("dongzuo");
        //创建高亮构造
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //设置高亮显示的前后标签
        highlightBuilder.preTags("<h3>");
        highlightBuilder.postTags("</h3>");
        //设置高亮显示的域
        highlightBuilder.field("title");
        //得到查询结果,这里不是matchall，而是根据字段的需求的查询
        SearchResponse searchResponse = searchRequestBuilder.setQuery(QueryBuilders.matchQuery("title","战"))
                .highlighter(highlightBuilder)
                .setFetchSource(new String[]{"title","price"},null)
                .execute()
                .actionGet();
        //得到查询hits
        SearchHits hits = searchResponse.getHits();
        //遍历hits
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            //取出高亮结果
            System.out.println(hit.getHighlightFields());
        }
    }

}
