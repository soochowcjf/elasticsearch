package com.java1234;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

/**
 * @author:chenjinfeng
 * @Date:2018/6/3
 * @Time:0:04
 * 测试多条件组合查询
 */
public class TestMultiSearch {

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
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(TestMultiSearch.host), TestMultiSearch.port));
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
     * 多条件组合查询 boolean
     * 模糊查询     must
     */
    @Test
    public void multiSearchMust()throws Exception {
        //得到查询请求构造
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("film").setTypes("dongzuo");
        //创建模糊查询条件   must
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("title","战");
        QueryBuilder queryBuilder2 = QueryBuilders.matchPhraseQuery("content","星球");
        SearchResponse searchResponse = searchRequestBuilder.setQuery(QueryBuilders.boolQuery()
                .must(queryBuilder)
                .must(queryBuilder2))
            .execute()
            .actionGet();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
    /**
     * 多条件组合查询 boolean
     * 模糊查询     mustNot
     */
    @Test
    public void multiSearchMustNot()throws Exception {
        //得到查询请求构造
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("film").setTypes("dongzuo");
        //创建模糊查询条件   mustNot
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("title","战");
        QueryBuilder queryBuilder2 = QueryBuilders.matchPhraseQuery("content","武士");
        SearchResponse searchResponse = searchRequestBuilder.setQuery(QueryBuilders.boolQuery()
                .must(queryBuilder)
                .mustNot(queryBuilder2))
            .execute()
            .actionGet();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
    /**
     * 多条件组合查询 boolean
     * 模糊查询     should
     */
    @Test
    public void multiSearchShould()throws Exception {
        //得到查询请求构造
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("film").setTypes("dongzuo");
        //创建模糊查询条件   mustNot
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("title","战");
        QueryBuilder queryBuilder2 = QueryBuilders.matchPhraseQuery("content","星球");
        //出版日期大于2018-01-01的
        QueryBuilder queryBuilder3 = QueryBuilders.rangeQuery("publishDate").gt("2018-01-01");
        SearchResponse searchResponse = searchRequestBuilder.setQuery(QueryBuilders.boolQuery()
                .must(queryBuilder)
                .should(queryBuilder2)
                .should(queryBuilder3))
            .execute()
            .actionGet();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getScore()+":"+hit.getSourceAsString());
        }
    }
    /**
     * 多条件组合查询 boolean
     * 模糊查询     filter
     */
    @Test
    public void multiSearchFilter()throws Exception {
        //得到查询请求构造
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("film").setTypes("dongzuo");
        //创建模糊查询条件   mustNot
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("title","战");
        //过滤价格小于等于40的
        QueryBuilder queryBuilder2 = QueryBuilders.rangeQuery("price").lte(40);
        SearchResponse searchResponse = searchRequestBuilder.setQuery(QueryBuilders.boolQuery()
                .must(queryBuilder)
                .filter(queryBuilder2))
                .execute()
                .actionGet();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
}
