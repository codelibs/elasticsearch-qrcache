package org.codelibs.qrcache.cache;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.qrcache.cache.QueryResultCache;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;

public class QueryResultCacheTest {
    @Test
    public void cachedIndex() throws Exception {
        ElasticsearchClusterRunner runner = new ElasticsearchClusterRunner();
        runner.build(new String[] { "-numOfNode", "1", "-indexStoreType", "ram" });
        QueryResultCache queryResultCache = QueryResultCache.get();

        runner.ensureGreen();

        assertThat(1, is(runner.getNodeSize()));
        Client client = runner.client();

        String index = "sample";
        String type = "data";
        runner.createIndex(
                index,
                ImmutableSettings.builder()
                        .put(QueryResultCache.INDEX_CACHE_QUERY_ENABLED, true)
                        .build());

        for (int i = 1; i <= 1000; i++) {
            IndexResponse indexResponse1 = runner.insert(index, type,
                    String.valueOf(i), "{\"id\":\"" + i + "\",\"msg\":\"test "
                            + i + "\"}");
            assertTrue(indexResponse1.isCreated());
        }

        assertThat(Long.valueOf(queryResultCache.cache.size()), is(0L));
        {
            SearchResponse searchResponse = client.prepareSearch("sample")
                    .setQuery(QueryBuilders.matchAllQuery()).execute()
                    .actionGet();
            SearchHits hits = searchResponse.getHits();
            assertThat(Long.valueOf(1000), is(hits.getTotalHits()));

        }
        
        Thread.sleep(500);
        
        assertThat(Long.valueOf(queryResultCache.cache.size()), is(1L));
        {
            SearchResponse searchResponse = client.prepareSearch("sample")
                    .setQuery(QueryBuilders.matchAllQuery()).execute()
                    .actionGet();
            SearchHits hits = searchResponse.getHits();
            assertThat(Long.valueOf(1000), is(hits.getTotalHits()));
        }
    }
}
