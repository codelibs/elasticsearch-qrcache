package org.codelibs.qrcache.cache;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryResultCacheTest {
    ElasticsearchClusterRunner runner;

    @Before
    public void setUp() throws Exception {
        runner = new ElasticsearchClusterRunner();
        runner.build(new String[] { "-numOfNode", "1", "-indexStoreType", "ram" });
        runner.ensureGreen();
    }

    @After
    public void tearDown() throws Exception {
        runner.close();
        runner.clean();
    }

    @Test
    public void cachedIndex() throws Exception {
        final QueryResultCache queryResultCache = QueryResultCache.get();

        assertThat(1, is(runner.getNodeSize()));

        final String index = "sample";
        final String type = "data";
        runner.createIndex(
                index,
                ImmutableSettings.builder()
                        .put(QueryResultCache.INDEX_CACHE_QUERY_ENABLED, true)
                        .build());
        {
            final QueryResultCacheStats stats = queryResultCache.stats();
            assertEquals(0, stats.getSize());
            assertEquals(0, stats.getTotal());
            assertEquals(0, stats.getHits());
            assertEquals(0, stats.getEvictions());
            assertEquals(0, stats.getRequestMemorySize().bytes());
            assertEquals(0, stats.getResponseMemorySize().bytes());
        }

        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, type,
                    String.valueOf(i), "{\"id\":\"" + i + "\",\"msg\":\"test "
                            + i + "\"}");
            assertTrue(indexResponse1.isCreated());
        }

        assertThat(Long.valueOf(queryResultCache.cache.size()), is(0L));
        {
            final SearchResponse searchResponse = runner.search(index, type,
                    QueryBuilders.matchAllQuery(), null, 0, 10);
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits());

            Thread.sleep(500);
            final QueryResultCacheStats stats = queryResultCache.stats();
            assertEquals(1, stats.getSize());
            assertEquals(1, stats.getTotal());
            assertEquals(0, stats.getHits());
            assertEquals(0, stats.getEvictions());
            assertNotEquals(0, stats.getRequestMemorySize().bytes());
            assertNotEquals(0, stats.getResponseMemorySize().bytes());
        }

        assertThat(Long.valueOf(queryResultCache.cache.size()), is(1L));
        {
            final SearchResponse searchResponse = runner.search(index, type,
                    QueryBuilders.matchAllQuery(), null, 0, 10);
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits());

            final QueryResultCacheStats stats = queryResultCache.stats();
            assertEquals(1, stats.getSize());
            assertEquals(2, stats.getTotal());
            assertEquals(1, stats.getHits());
            assertEquals(0, stats.getEvictions());
            assertNotEquals(0, stats.getRequestMemorySize().bytes());
            assertNotEquals(0, stats.getResponseMemorySize().bytes());
        }

        queryResultCache.clear();

        {
            final QueryResultCacheStats stats = queryResultCache.stats();
            assertEquals(0, stats.getSize());
            assertEquals(0, stats.getTotal());
            assertEquals(0, stats.getHits());
            assertEquals(0, stats.getEvictions());
            assertEquals(0, stats.getRequestMemorySize().bytes());
            assertEquals(0, stats.getResponseMemorySize().bytes());
        }

        assertEquals(
                200,
                Curl.put(
                        runner.node(),
                        "/"
                                + index
                                + "/_settings?index.cache.query_result.enable=false")
                        .execute().getHttpStatusCode());

        assertThat(Long.valueOf(queryResultCache.cache.size()), is(0L));
        {
            Thread.sleep(500);
            final SearchResponse searchResponse1 = runner.search(index, type,
                    QueryBuilders.matchAllQuery(), null, 0, 10);
            final SearchHits hits1 = searchResponse1.getHits();
            assertEquals(1000, hits1.getTotalHits());
            Thread.sleep(500);
            final SearchResponse searchResponse2 = runner.search(index, type,
                    QueryBuilders.matchAllQuery(), null, 0, 10);
            final SearchHits hits2 = searchResponse2.getHits();
            assertEquals(1000, hits2.getTotalHits());
            Thread.sleep(500);

            final QueryResultCacheStats stats = queryResultCache.stats();
            assertEquals(0, stats.getSize());
            assertEquals(0, stats.getTotal());
            assertEquals(0, stats.getHits());
            assertEquals(0, stats.getEvictions());
            assertEquals(0, stats.getRequestMemorySize().bytes());
            assertEquals(0, stats.getResponseMemorySize().bytes());
        }

        assertEquals(
                200,
                Curl.put(
                        runner.node(),
                        "/"
                                + index
                                + "/_settings?index.cache.query_result.enable=true")
                        .execute().getHttpStatusCode());

        assertThat(Long.valueOf(queryResultCache.cache.size()), is(0L));
        {
            Thread.sleep(500);
            final SearchResponse searchResponse1 = runner.search(index, type,
                    QueryBuilders.matchAllQuery(), null, 0, 10);
            final SearchHits hits1 = searchResponse1.getHits();
            assertEquals(1000, hits1.getTotalHits());
            Thread.sleep(500);
            final SearchResponse searchResponse2 = runner.search(index, type,
                    QueryBuilders.matchAllQuery(), null, 0, 10);
            final SearchHits hits2 = searchResponse2.getHits();
            assertEquals(1000, hits2.getTotalHits());
            Thread.sleep(500);

            final QueryResultCacheStats stats = queryResultCache.stats();
            assertEquals(1, stats.getSize());
            assertEquals(2, stats.getTotal());
            assertEquals(1, stats.getHits());
            assertEquals(0, stats.getEvictions());
            assertNotEquals(0, stats.getRequestMemorySize().bytes());
            assertNotEquals(0, stats.getResponseMemorySize().bytes());
        }
    }
}
