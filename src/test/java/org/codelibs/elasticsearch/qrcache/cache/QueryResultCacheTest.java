package org.codelibs.elasticsearch.qrcache.cache;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
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
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int index, final Builder settingsBuilder) {
                settingsBuilder.put("engine.filter.refresh", true);
            }
        }).build(newConfigs().numOfNode(1).ramIndexStore());
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
        final Client client = runner.client();

        final String index = "sample";
        final String type = "data";
        runner.createIndex(
                index,
                ImmutableSettings.builder()
                        .put(QueryResultCache.INDEX_CACHE_QUERY_ENABLED, true)
                        .put("index.number_of_shards", 1).build());
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
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(QueryBuilders.matchAllQuery()).execute()
                    .actionGet();
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
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(QueryBuilders.matchAllQuery()).execute()
                    .actionGet();
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

    }
}
