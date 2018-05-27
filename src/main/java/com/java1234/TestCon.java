package com.java1234;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * author:chenjinfeng
 * Date:2018/5/27
 * Time:18:02
 * 测试es的连接
 */
public class TestCon {
    //服务器地址
    private static String host = "192.168.25.131";
    //端口号
    private static int port = 9300;
    private TransportClient client=null;

    public static void main(String[] args) throws Exception {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(TestCon.host), TestCon.port));
        System.out.println(client);
        client.close();
    }
}
