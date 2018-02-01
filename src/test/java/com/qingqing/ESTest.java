package com.qingqing;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class ESTest {

    private TransportClient client;
    private static final String INDEX = "twitter";
    private static final String TYPE = "tweet";
    private static final String CLUSTER_NAME = "elasticsearch-cluster-tst";

    @Before
    @SuppressWarnings({"unchecked"})
    public void before() {

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", CLUSTER_NAME)
                .put("client.transport.sniff", true)
                .put("client.transport.ignore_cluster_name", true)
                .build();
        client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress("172.20.13.71", 9300));
        System.out.println("connect succ ..." + client);
    }

    @Test
    public void testInfo() {
        ImmutableList<DiscoveryNode> discoveryNodes = client.connectedNodes();
        for (DiscoveryNode node : discoveryNodes) {
            System.out.println("node id:" + node.id());
            System.out.println("node hostname:" + node.getHostName());
            System.out.println("node name:" + node.getName());
            System.out.println("node address:" + node.getAddress());
        }
    }

    /**
     * 生成Json
     * 官方文档提供了4中生成Json数据的方式：手动拼装、Map、第三方库Jackson、内置的XContentFactory.jsonBuilder()
     * 各种方式的底层都是转换为byte[]
     */
    @Test
    public void testJson() {
        // 方式1：手动拼装
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        System.out.println(json);

        // 方式2: 使用Map  底层返回也是byte[]??
        Map<String, Object> json2 = new HashMap<String, Object>();
        json2.put("user", "kimchy");
        json2.put("postDate", new Date());
        json2.put("message", "trying out Elasticsearch");
        System.out.println(json2);

        // 方式3：使用第三方库fastjson
        JSONObject json3 = new JSONObject();
        json3.put("user", "kimchy");
        json3.put("postDate", new Date());
        json3.put("message", "trying out elasticsearch");
        System.out.println(json3);

        // 方式4: ES内置工具
        // XContentBuilder可以添加许多类型：对象、基本类型、另一个XContentBuilder
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("user", "kimchy")
                    .field("age", 20)
                    .field("postDate", new Date())
                    .field("message", "trying out Elasticsearch")
                    .endObject();
            System.out.println(builder.string());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 向index twitter 插入一条document
    // 注意：首先确保你已创建了index twitter，
    // 否则会报IndexMissingException: [twitter] missing
    @Test
    public void testIndexDocument() {
        // 创建一条document
        XContentBuilder builder = null;
        try {
            builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("user", "geyang")
                    .field("age", 20)
                    .field("postDate", new Date())
                    .field("message", "trying out Elasticsearch")
                    .endObject();
            System.out.println(builder.string());

            IndexResponse indexResponse = client.prepareIndex(INDEX, TYPE, "1")
                    .setSource(builder)
                    .execute().actionGet();

            // 创建的Index
            String index = indexResponse.getIndex();
            // 创建index 的type
            String type = indexResponse.getType();
            // document的版本，如果第一次索引，version值为1
            long version = indexResponse.getVersion();
            // 是否创建，如果第一次索引Document , created 为 true，更新操作为false
            boolean created = indexResponse.isCreated();

            System.out.println("index:" + index + "type:" + type + "version:" + version + "created:" + created);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 获取一条记录
    @Test
    public void testGetDcoument() {
        GetResponse response = client.prepareGet(INDEX, TYPE, "1")
                .execute()
                .actionGet();

        System.out.println(response.getSourceAsString());
    }

    // 更新一条记录
    @Test
    public void testUpdateDocument() {

        XContentBuilder builder = null;

        try {
            builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("user", "geyang")
                    .field("age", 21)
                    .field("postDate", new Date())
                    .field("message", "update geyang")
                    .endObject();
            System.out.println(builder.string());
        } catch (IOException e) {
            e.printStackTrace();
        }

        UpdateResponse updateResponse = client.prepareUpdate(INDEX, TYPE, "1")
                .setDoc(builder)
                .execute().actionGet();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        System.out.println("id:" + id + "version:" + version);

    }

    private XContentBuilder buildJson(Map<Object, Object> param) {
        XContentBuilder builder = null;

        try {
            builder = XContentFactory.jsonBuilder().startObject();
            for (Map.Entry<Object, Object> entry : param.entrySet()) {
                builder.field(entry.getKey().toString(), entry.getValue());
            }
            builder.endObject();

            System.out.println(builder.string());

        } catch (IOException e) {
            e.printStackTrace();
        }
        return builder;
    }

    // 删除一条记录
    @Test
    public void testDeleteDocument() {
        DeleteResponse response = client.prepareDelete(INDEX, TYPE, "1")
                .execute()
                .actionGet();

        String id = response.getId();
        boolean found = response.isFound();

        System.out.println("id: " + id + "found:" + found);
    }

    // bulk request ：在一条请求中做多个索引、删除索引等的操作
    @Test
    public void testBulk() {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        Map<Object, Object> param1 = new LinkedHashMap<Object, Object>();
        param1.put("user", "geyang");
        param1.put("age", 21);
        param1.put("postDate", new Date());
        param1.put("message", "bulk1 geyang");
        XContentBuilder builder1 = buildJson(param1);

        Map<Object, Object> param2 = new LinkedHashMap<Object, Object>();
        param2.put("user", "qingqing");
        param2.put("age", 21);
        param2.put("postDate", new Date());
        param2.put("message", "bulk2 qingqing");
        XContentBuilder builder2 = buildJson(param2);

        bulkRequest.add(client.prepareIndex(INDEX, TYPE, "1").setSource(builder1));
        bulkRequest.add(client.prepareIndex(INDEX, TYPE, "2").setSource(builder2));

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        for ( BulkItemResponse response: bulkResponse.getItems()) {
            String id = response.getId();
            boolean failed = response.isFailed();
            ActionResponse actionResponse = response.getResponse();
            System.out.println("id:" + id + "failed:" + failed + actionResponse);
        }

        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }
    }

    // bulk请求 ：索引一条记录，再删除一条记录
    @Test
    public void testBulk2() {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        Map<Object, Object> param1 = new LinkedHashMap<Object, Object>();
        param1.put("user", "geyang");
        param1.put("age", 21);
        param1.put("postDate", new Date());
        param1.put("message", "bulk1 geyang");
        XContentBuilder builder1 = buildJson(param1);

        bulkRequest.add(client.prepareIndex(INDEX, TYPE, "1").setSource(builder1));
        bulkRequest.add(client.prepareDelete(INDEX, TYPE, "1"));

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
//        System.out.println(bulkResponse.getHeaders());
        for ( BulkItemResponse response: bulkResponse.getItems()) {
            String id = response.getId();
            boolean failed = response.isFailed();
            ActionResponse actionResponse = response.getResponse();
            System.out.println("id:" + id + "failed:" + failed + actionResponse);
        }
    }

}
