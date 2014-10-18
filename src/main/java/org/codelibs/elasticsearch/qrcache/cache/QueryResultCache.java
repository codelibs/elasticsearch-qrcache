package org.codelibs.elasticsearch.qrcache.cache;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;
import static org.elasticsearch.common.Strings.hasLength;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.codelibs.elasticsearch.qrcache.QueryResultCachePlugin;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.cache.Weigher;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.threadpool.ThreadPool;

public class QueryResultCache extends AbstractComponent implements
        RemovalListener<QueryResultCache.Key, BytesReference> {

    public static final String INDEX_CACHE_QUERY_ENABLED = "index.cache.query_result.enable";

    public static final String INDICES_CACHE_QUERY_CLEAN_INTERVAL = "indices.cache.query_result.clean_interval";

    public static final String INDICES_CACHE_QUERY_SIZE = "indices.cache.query_result.size";

    public static final String INDICES_CACHE_QUERY_EXPIRE = "indices.cache.query_result.expire";

    protected final ESLogger logger;

    private final ThreadPool threadPool;

    private ClusterService clusterService;

    private final TimeValue cleanInterval;

    private final Reaper reaper;

    private volatile String size;

    private volatile TimeValue expire;

    protected volatile Cache<Key, BytesReference> cache;

    private volatile Set<String> indicesToClean = ConcurrentCollections
            .newConcurrentSet();

    private volatile CounterMetric hitsMetric = new CounterMetric();

    private volatile CounterMetric totalMetric = new CounterMetric();

    private volatile CounterMetric evictionsMetric = new CounterMetric();

    @Inject
    public QueryResultCache(final Settings settings,
            final ClusterService clusterService, final ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.logger = Loggers.getLogger(
                QueryResultCachePlugin.INDEX_LOGGER_NAME, settings);

        cleanInterval = settings.getAsTime(INDICES_CACHE_QUERY_CLEAN_INTERVAL,
                TimeValue.timeValueSeconds(10));
        size = settings.get(INDICES_CACHE_QUERY_SIZE, "1%");
        expire = settings.getAsTime(INDICES_CACHE_QUERY_EXPIRE, null);
        buildCache();

        reaper = new Reaper();
        threadPool.schedule(cleanInterval, ThreadPool.Names.SAME, reaper);

    }

    private void buildCache() {
        final long sizeInBytes = MemorySizeValue
                .parseBytesSizeValueOrHeapRatio(size).bytes();

        final CacheBuilder<Key, BytesReference> cacheBuilder = CacheBuilder
                .newBuilder().maximumWeight(sizeInBytes)
                .weigher(new QueryCacheWeigher()).removalListener(this);

        cacheBuilder.concurrencyLevel(16);

        if (expire != null) {
            cacheBuilder.expireAfterAccess(expire.millis(),
                    TimeUnit.MILLISECONDS);
        }

        cache = cacheBuilder.build();
    }

    public QueryResultCacheStats stats() {
        long reqMemSize = 0;
        long resMemSize = 0;
        for (final Map.Entry<Key, BytesReference> entry : cache.asMap()
                .entrySet()) {
            reqMemSize += entry.getKey().ramBytesUsed();
            resMemSize += entry.getValue().length();
        }
        return new QueryResultCacheStats(cache.size(), reqMemSize, resMemSize,
                totalMetric.count(), hitsMetric.count(),
                evictionsMetric.count());
    }

    private static class QueryCacheWeigher implements
            Weigher<Key, BytesReference> {
        @Override
        public int weigh(final Key key, final BytesReference value) {
            return (int) (key.ramBytesUsed() + value.length());
        }
    }

    public void close() {
        reaper.close();
        cache.invalidateAll();
    }

    @Override
    public void onRemoval(
            final RemovalNotification<Key, BytesReference> notification) {
        if (notification.getKey() == null) {
            return;
        }
        evictionsMetric.inc();
    }

    public boolean canCache(final SearchRequest request) {
        if (hasLength(request.templateSource())) {
            return false;
        }
        final String[] indices = request.indices();
        if (indices == null || indices.length == 0) {
            return false;
        }
        for (final String indexName : indices) {
            final IndexMetaData index = clusterService.state().getMetaData()
                    .index(indexName);
            if (index == null) {
                return false;
            }
            if (!index.settings().getAsBoolean(INDEX_CACHE_QUERY_ENABLED,
                    Boolean.FALSE)) {
                return false;
            }
        }
        switch (request.searchType()) {
        case SCAN:
        case COUNT:
            return false;
        default:
            break;
        }
        return true;
    }

    public ActionListener execute(final SearchRequest request,
            final ActionListener<SearchResponse> listener,
            final ActionFilterChain chain) {
        try {
            final Key key = buildKey(request);
            totalMetric.inc();
            final BytesReference value = cache.getIfPresent(key);
            if (value != null) {
                hitsMetric.inc();
                final SearchResponse response = readFromCache(value);
                if (logger.isDebugEnabled()) {
                    logger.debug("Read cached response for {}/{}/{}: {}",
                            response.getTotalShards(), response
                                    .getSuccessfulShards(), response
                                    .getFailedShards(), response.getHits()
                                    .getTotalHits());
                }
                listener.onResponse(response);
            } else {
                return new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(final SearchResponse response) {
                        onCache(key, response);
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        listener.onFailure(e);
                    }
                };
            }
        } catch (final IOException e) {
            listener.onFailure(e);
        }
        return null;
    }

    private void onCache(final Key key, final SearchResponse response) {
        if (response.isTimedOut()) {
            return;
        }

        try {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                cache.get(key, new Callable<BytesReference>() {
                                    @Override
                                    public BytesReference call()
                                            throws Exception {
                                        final BytesStreamOutput out = new BytesStreamOutput();
                                        response.writeTo(out);
                                        return out.bytes();
                                    }
                                });
                            } catch (final ExecutionException e) {
                                logger.warn(
                                        "Failed to write a responses to a buffer.",
                                        e);
                            }
                            if (logger.isDebugEnabled()) {
                                logger.debug(
                                        "Wrote cached response for {}/{}/{}: {}",
                                        response.getTotalShards(), response
                                                .getSuccessfulShards(),
                                        response.getFailedShards(), response
                                                .getHits().getTotalHits());
                            }
                        }
                    });
        } catch (final EsRejectedExecutionException ex) {
            logger.warn("Can not run a process to store a cache", ex);
        }
    }

    private SearchResponse readFromCache(final BytesReference value)
            throws IOException {
        final long startTime = System.nanoTime();
        final StreamInput in = value.streamInput();
        Map<String, Object> headers = null;
        if (in.readBoolean()) {
            headers = in.readMap();
        }
        final InternalSearchResponse internalResponse = new InternalSearchResponse(
                null, null, null, null, false, null);
        internalResponse.readFrom(in);
        final int totalShards = in.readVInt();
        final int successfulShards = in.readVInt();
        final int size = in.readVInt();
        ShardSearchFailure[] shardFailures;
        if (size == 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[size];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = readShardSearchFailure(in);
            }
        }
        final String scrollId = in.readOptionalString();
        final long tookInMillis = (System.nanoTime() - startTime) / 1000000;
        final SearchResponse response = new SearchResponse(internalResponse,
                scrollId, totalShards, successfulShards, tookInMillis,
                shardFailures);
        if (headers != null) {
            for (final Map.Entry<String, Object> entry : headers.entrySet()) {
                response.putHeader(entry.getKey(), entry.getValue());
            }
        }
        return response;
    }

    public void clear(final String index) {
        if (logger.isDebugEnabled()) {
            logger.debug("Cache for {} will be invalidated.", index);
        }
        indicesToClean.add(index);
    }

    public void clear(final String... indices) {
        if (indices == null || indices.length == 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Invalidating all cache.");
            }
            indicesToClean.clear();
            cache.invalidateAll();
            totalMetric = new CounterMetric();
            hitsMetric = new CounterMetric();
            evictionsMetric = new CounterMetric();
        } else {
            for (final String index : indices) {
                clear(index);
            }
            try {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(
                        new Runnable() {
                            @Override
                            public void run() {
                                reaper.reap();
                            }
                        });
            } catch (final EsRejectedExecutionException ex) {
                logger.debug("Can not run ReaderCleaner - execution rejected",
                        ex);
            }
        }
    }

    public static class Key implements Accountable {
        public final BytesReference value;

        Key(final BytesReference value) {
            this.value = value;
        }

        public String[] indices() {
            try (BytesStreamInput in = new BytesStreamInput(value)) {
                if (in.readBoolean()) {
                    in.readMap();
                }
                if (in.getVersion().before(Version.V_1_2_0)) {
                    in.readByte();
                }
                in.readByte();

                final String[] indices = new String[in.readVInt()];
                for (int i = 0; i < indices.length; i++) {
                    indices[i] = in.readString();
                }
                return indices;
            } catch (final IOException e) {
                return new String[0];
            }
        }

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF
                    + RamUsageEstimator.NUM_BYTES_LONG + value.length();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (value == null ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Key other = (Key) obj;
            if (value == null) {
                if (other.value != null) {
                    return false;
                }
            } else if (!value.equals(other.value)) {
                return false;
            }
            return true;
        }

    }

    private static Key buildKey(final SearchRequest request) throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        return new Key(out.bytes().copyBytesArray());
    }

    private class Reaper implements Runnable {

        private volatile boolean closed;

        void close() {
            closed = true;
        }

        @Override
        public void run() {
            if (closed) {
                return;
            }
            if (indicesToClean.isEmpty()) {
                schedule();
                return;
            }
            try {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(
                        new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    reap();
                                } finally {
                                    schedule();
                                }
                            }
                        });
            } catch (final EsRejectedExecutionException ex) {
                logger.debug("Can not run ReaderCleaner - execution rejected",
                        ex);
                schedule();
            }
        }

        private void schedule() {
            boolean success = false;
            while (!success) {
                if (closed) {
                    break;
                }
                try {
                    threadPool.schedule(cleanInterval, ThreadPool.Names.SAME,
                            this);
                    success = true;
                } catch (final EsRejectedExecutionException ex) {
                    logger.warn("Can not schedule Reaper - execution rejected",
                            ex);
                    try {
                        Thread.sleep(1000);
                    } catch (final InterruptedException e) {
                        // ignore
                    }
                }
            }
        }

        private void reap() {
            if (logger.isDebugEnabled()) {
                logger.debug("Clearing cached responses...");
            }

            final Set<String> currentIndicesToClean = indicesToClean;
            indicesToClean = ConcurrentCollections.newConcurrentSet();
            final List<Key> keyToClean = new ArrayList<>();

            for (final Key key : cache.asMap().keySet()) {
                final String[] indices = key.indices();
                if (indices == null || indices.length == 0) {
                    keyToClean.add(key);
                    if (logger.isDebugEnabled()) {
                        try {
                            final StreamInput in = key.value.streamInput();
                            final SearchRequest request = new SearchRequest();
                            request.readFrom(in);
                            logger.debug("Invalidating cache: {}", request);
                        } catch (final IOException e) {
                            logger.warn("Failed to read a search request.", e);
                        }
                    }
                } else {
                    for (final String index : indices) {
                        if (currentIndicesToClean.contains(index)) {
                            keyToClean.add(key);
                            if (logger.isDebugEnabled()) {
                                try {
                                    final StreamInput in = key.value
                                            .streamInput();
                                    final SearchRequest request = new SearchRequest();
                                    request.readFrom(in);
                                    logger.debug("Invalidating cache: {}",
                                            request);
                                } catch (final IOException e) {
                                    logger.warn(
                                            "Failed to read a search request.",
                                            e);
                                }
                            }
                            break;
                        }
                    }
                }
            }

            if (!keyToClean.isEmpty()) {
                cache.invalidateAll(keyToClean);
            }
        }
    }
}
