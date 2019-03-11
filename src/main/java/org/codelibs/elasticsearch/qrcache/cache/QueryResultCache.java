package org.codelibs.elasticsearch.qrcache.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor.AsyncSender;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

public class QueryResultCache implements RemovalListener<QueryResultCache.Key, BytesReference> {
    private static final Logger logger = LogManager.getLogger(QueryResultCache.class);

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final TimeValue cleanInterval;

    private final Reaper reaper;

    protected volatile Cache<Key, BytesReference> cache;

    private volatile Set<String> indicesToClean = ConcurrentCollections.newConcurrentSet();

    private volatile CounterMetric hitsMetric = new CounterMetric();

    private volatile CounterMetric totalMetric = new CounterMetric();

    private volatile CounterMetric evictionsMetric = new CounterMetric();

    public static final Setting<TimeValue> CLEAN_INTERVAL_SETTING =
            Setting.timeSetting("query_result_cache.clean_interval", TimeValue.timeValueSeconds(10), Property.NodeScope);

    public static final Setting<ByteSizeValue> MAX_SIZE_SETTING =
            Setting.memorySizeSetting("query_result_cache.max_size", "1%", Property.NodeScope);

    public static final Setting<TimeValue> EXPIRE_SETTING =
            Setting.timeSetting("query_result_cache.expire", TimeValue.ZERO, Property.NodeScope);

    public static final Setting<Boolean> INDEX_ENABLED_SETTING =
            Setting.boolSetting("index.query_result_cache.enabled", false, Property.IndexScope);

    public QueryResultCache(final Settings settings, final ClusterService clusterService, final ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        cleanInterval = CLEAN_INTERVAL_SETTING.get(settings);
        buildCache(settings);

        reaper = new Reaper();
        threadPool.schedule(cleanInterval, ThreadPool.Names.SAME, reaper);

    }

    private void buildCache(Settings settings) {

        final long maxWeight = MAX_SIZE_SETTING.get(settings).getBytes();
        final CacheBuilder<Key, BytesReference> cacheBuilder =
                CacheBuilder.newBuilder().maximumWeight(maxWeight).weigher(new QueryCacheWeigher()).removalListener(this);

        cacheBuilder.concurrencyLevel(16);

        final TimeValue expire = EXPIRE_SETTING.get(settings);
        if (!expire.equals(TimeValue.ZERO)) {
            cacheBuilder.expireAfterAccess(expire.millis(), TimeUnit.MILLISECONDS);
        }

        cache = cacheBuilder.build();
    }

    public QueryResultCacheStats stats() {
        long reqMemSize = 0;
        long resMemSize = 0;
        for (final Map.Entry<Key, BytesReference> entry : cache.asMap().entrySet()) {
            reqMemSize += entry.getKey().ramBytesUsed();
            resMemSize += entry.getValue().length();
        }
        return new QueryResultCacheStats(cache.size(), reqMemSize, resMemSize, totalMetric.count(), hitsMetric.count(),
                evictionsMetric.count());
    }

    private static class QueryCacheWeigher implements Weigher<Key, BytesReference> {
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
    public void onRemoval(final RemovalNotification<Key, BytesReference> notification) {
        if (notification.getKey() == null) {
            return;
        }
        evictionsMetric.inc();
    }

    public boolean canCache(final ShardSearchTransportRequest request) {
        final ShardId shardId = request.shardId();
        final String indexName = shardId.getIndexName();
        final IndexMetaData index = clusterService.state().getMetaData().index(indexName);
        if (index == null) {
            return false;
        }
        if (!INDEX_ENABLED_SETTING.get(index.getSettings())) {
            return false;
        }
        return true;
    }

    public <T extends TransportResponse> void sendCacheRequest(final Connection connection, final String action,
            final ShardSearchTransportRequest request, final TransportRequestOptions options, final TransportResponseHandler<T> handler,
            final AsyncSender sender) {
        try {
            final Key key = new Key(request);
            totalMetric.inc();
            final BytesReference value = cache.getIfPresent(key);
            if (value != null) {
                hitsMetric.inc();
                final QuerySearchResult response = readFromCache(value);
                if (logger.isDebugEnabled()) {
                    logger.debug("Read cached response for {}/{}/{}: {}", response.getShardIndex(), response.getRequestId(),
                            response.getTotalHits());
                }
                @SuppressWarnings("unchecked")
                T res = (T) response;
                handler.handleResponse(res);
            } else {
                sender.sendRequest(connection, action, request, options, new TransportResponseHandler<T>() {

                    @Override
                    public T read(final StreamInput in) throws IOException {
                        return handler.read(in);
                    }

                    @Override
                    public void handleResponse(final T response) {
                        onCache(key, (QuerySearchResult) response);
                        handler.handleResponse(response);
                    }

                    @Override
                    public void handleException(final TransportException exp) {
                        handler.handleException(exp);
                    }

                    @Override
                    public String executor() {
                        return handler.executor();
                    }
                });
            }
        } catch (final IOException e) {
            handler.handleException(new TransportException(e));
        }
    }

    private void onCache(final Key key, final QuerySearchResult result) {
        if (result.searchTimedOut()) {
            return;
        }

        try (final BytesStreamOutput out = new BytesStreamOutput()) {
            result.writeTo(out);
            cache.put(key, out.bytes());
        } catch (final IOException e) {
            logger.warn("Failed to write a responses to the cache.", e);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Wrote cached response for {}/{}: {}", result.getShardIndex(), result.getRequestId(), result.getTotalHits());
        }
    }

    private QuerySearchResult readFromCache(final BytesReference value) throws IOException {
        try (final StreamInput in = value.streamInput()) {
            final QuerySearchResult result = new QuerySearchResult();
            result.readFrom(in);
            return result;
        }
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
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> reaper.reap());
            } catch (final EsRejectedExecutionException ex) {
                logger.debug("Can not run ReaderCleaner - execution rejected", ex);
            }
        }
    }

    public static class Key implements Accountable {
        public final BytesReference value;

        public Key(final ShardSearchTransportRequest request) throws IOException {
            final ShardId shardId = request.shardId();
            final SearchSourceBuilder source = request.source();
            try (final BytesStreamOutput out = new BytesStreamOutput()) {
                shardId.writeTo(out);
                source.writeTo(out);
                this.value = out.bytes();
            }
        }

        public ShardId shardId() {
            try (StreamInput in = value.streamInput()) {
                return ShardId.readShardId(in);
            } catch (final IOException e) {
                return null;
            }
        }

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF + Long.BYTES + value.length();
        }

        @Override
        public int hashCode() {
            return value.hashCode();
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
            return value.equals(other.value);
        }

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
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                    try {
                        reap();
                    } finally {
                        schedule();
                    }
                });
            } catch (final EsRejectedExecutionException ex) {
                logger.debug("Can not run ReaderCleaner - execution rejected", ex);
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
                    threadPool.schedule(cleanInterval, ThreadPool.Names.SAME, this);
                    success = true;
                } catch (final EsRejectedExecutionException ex) {
                    logger.warn("Can not schedule Reaper - execution rejected", ex);
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
                final ShardId shardId = key.shardId();
                if (shardId == null) {
                    keyToClean.add(key); // invalid key?
                } else {
                    if (currentIndicesToClean.contains(shardId.getIndexName())) {
                        keyToClean.add(key);
                        if (logger.isDebugEnabled()) {
                            try (final StreamInput in = key.value.streamInput()) {
                                ShardId.readShardId(in);
                                SearchSourceBuilder source = new SearchSourceBuilder(in);
                                logger.debug("Invalidating cache: {}", source);
                            } catch (final IOException e) {
                                logger.warn("Failed to read a search request.", e);
                            }
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