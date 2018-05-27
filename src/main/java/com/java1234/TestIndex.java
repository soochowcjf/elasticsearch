package com.java1234;

import com.google.gson.JsonObject;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
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
public class TestIndex {
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
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(TestIndex.host), TestIndex.port));
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
     * 添加索引
     */
    @Test
    public void addIndex() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name","java编程思想");
        jsonObject.addProperty("publishDate","2013-11-22");
        jsonObject.addProperty("price",100);

        IndexResponse response = client.prepareIndex("book", "java", "1").setSource(jsonObject.toString(),XContentType.JSON).get();
        System.out.println("索引名称："+response.getIndex());
        System.out.println("类型："+response.getType());
        System.out.println("文档ID："+response.getId());
        System.out.println("当前实例状态："+response.status());
    }

    /**
     * 根据id获取文档
     */
    @Test
    public void getDocById() throws Exception{
        GetResponse response = client.prepareGet("book", "java", "1").get();
        System.out.println(response.getSourceAsString());
    }
    /**
     * 根据id修改文档
     */
    @Test
    public void testUpdate()throws Exception{
        JsonObject jsonObject=new JsonObject();
        jsonObject.addProperty("name", "java编程思想2");
        jsonObject.addProperty("publishDate", "2012-11-12");
        jsonObject.addProperty("price", 102);

        UpdateResponse response=client.prepareUpdate("book", "java", "1").setDoc(jsonObject.toString(), XContentType.JSON).get();
        System.out.println("索引名称："+response.getIndex());
        System.out.println("类型："+response.getType());
        System.out.println("文档ID："+response.getId());
        System.out.println("当前实例状态："+response.status());
    }
    /**
     * 根据id删除文档
     */
    @Test
    public void testDelete()throws Exception{
        DeleteResponse response=client.prepareDelete("book", "java", "1").get();
        System.out.println("索引名称："+response.getIndex());
        System.out.println("类型："+response.getType());
        System.out.println("文档ID："+response.getId());
        System.out.println("当前实例状态："+response.status());
    }



}
