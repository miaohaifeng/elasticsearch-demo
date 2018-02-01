package com.qingqing;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class ESearchTest {

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
                    .field("user", "qingqing")
                    .field("age", 22)
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

    @Test
    public void testIndexMutilDocument() {

        Map<Object, Object> param = new LinkedHashMap<Object, Object>();
        for (int i = 0; i < 3; i++) {
            param.put("user", "geyang" + i);
            param.put("age", 20 + i);
            param.put("postDate", new Date());
            param.put("message", "insert " + i + "into index");

            XContentBuilder buildJson = buildJson(param);

            IndexResponse indexResponse = client.prepareIndex(INDEX, TYPE, String.valueOf(i))
                    .setSource(buildJson)
                    .execute().actionGet();

            System.out.println("id:" + indexResponse.getId() + "created:" + indexResponse.isCreated());
        }
    }

    @Test
    public void testSearchDefault() {

        SearchResponse response = client.prepareSearch().execute().actionGet();
        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    @Test
    public void testSearch() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("multi", "test"))             // Query
                .setPostFilter(FilterBuilders.rangeFilter("age").from(12).to(18))   // Filter
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();
    }

    // match_all search
    @Test
    public void testSearch1() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())             // Query
                .execute()
                .actionGet();

        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    // term search
    @Test
    public void testSearch2() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("age", 20))             // Query
                .execute()
                .actionGet();

        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // 输出json格式结果
            System.out.println(hit.getSourceAsString());
        }
    }

    // match search
    @Test
    public void testMatchSearch() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("user", "geyang0"))             // Query
                .execute()
                .actionGet();

        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // 输出json格式结果
            System.out.println(hit.getSourceAsString());
        }
    }

    // filter range and term query
    @Test
    public void testFilterSearch() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("user", "geyang1"))             // Query
                .setPostFilter(FilterBuilders.rangeFilter("age").from(10).to(30))   // Filter
                .execute()
                .actionGet();

        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // 输出json格式结果
            System.out.println(hit.getSourceAsString());
        }
    }

    // bool query
    @Test
    public void testBoolSearch() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("user", "geyang1")))             // Query
                .setPostFilter(FilterBuilders.rangeFilter("age").from(10).to(30))   // Filter
                .execute()
                .actionGet();

        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // 输出json格式结果
            System.out.println(hit.getSourceAsString());
        }
    }
}
