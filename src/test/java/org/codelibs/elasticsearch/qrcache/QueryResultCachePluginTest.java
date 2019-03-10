package org.codelibs.elasticsearch.qrcache;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.util.Map;

import org.codelibs.curl.CurlResponse;
import org.codelibs.elasticsearch.qrcache.cache.QueryResultCache;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.EcrCurl;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;

import junit.framework.TestCase;

public class QueryResultCachePluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    @Override
    protected void setUp() throws Exception {
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.putList("discovery.zen.ping.unicast.hosts", "localhost:9301-9310");
            }
        }).build(newConfigs().clusterName("es-cl-run-" + System.currentTimeMillis())
                .pluginTypes("org.codelibs.elasticsearch.qrcache.QueryResultCachePlugin").numOfNode(3));

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_plugin() throws Exception {
        final Node node = runner.node();
        final String index = "test_index";
        final String type = "test_type";

        // create an index
        runner.createIndex(index, Settings.builder().put(QueryResultCache.INDEX_ENABLED_SETTING.getKey(), true).build());
        runner.ensureYellow(index);

        // create a mapping
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(type)//
                .startObject("properties")//

                // id
                .startObject("id")//
                .field("type", "keyword")//
                .endObject()//

                // msg
                .startObject("msg")//
                .field("type", "text")//
                .endObject()//

                // order
                .startObject("order")//
                .field("type", "long")//
                .endObject()//

                // @timestamp
                .startObject("@timestamp")//
                .field("type", "date")//
                .endObject()//

                .endObject()//
                .endObject()//
                .endObject();
        runner.createMapping(index, type, mappingBuilder);

        if (!runner.indexExists(index)) {
            fail();
        }

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, type, String.valueOf(i), "{\"id\":\"" + i + "\",\"msg\":\"test " + i
                    + " a" + (i % 100) + " b" + (i % 10) + "\",\"order\":" + i + ",\"@timestamp\":\"2000-01-01T00:00:00\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        runner.refresh();

        try (CurlResponse curlResponse = EcrCurl.get(node, "/" + index + "/_search").param("q", "*:*").execute()) {
            final String content = curlResponse.getContentAsString();
            assertNotNull(content);
            final Map<String, Object> contentMap = curlResponse.getContent(EcrCurl.jsonParser);
            assertEquals(index, contentMap.get("index"));
        }

        /*
        try (CurlResponse curlResponse = EcrCurl.get(node, "/" + index + "/_queryRewriter").execute()) {
            final String content = curlResponse.getContentAsString();
            assertNotNull(content);
            final Map<String, Object> contentMap = curlResponse.getContent(EcrCurl.jsonParser);
            assertEquals(index, contentMap.get("index"));
            assertTrue(contentMap.get("description").toString().startsWith("This is a elasticsearch-query-rewriter response:"));
        }
        
        try (CurlResponse curlResponse = EcrCurl.get(node, "/_queryRewriter").execute()) {
            final String content = curlResponse.getContentAsString();
            assertNotNull(content);
            final Map<String, Object> contentMap = curlResponse.getContent(EcrCurl.jsonParser);
            assertFalse(contentMap.containsKey("index"));
            assertTrue(contentMap.get("description").toString().startsWith("This is a elasticsearch-query-rewriter response:"));
        }
        */
    }
}
