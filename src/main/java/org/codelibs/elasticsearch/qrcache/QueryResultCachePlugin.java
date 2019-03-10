package org.codelibs.elasticsearch.qrcache;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.codelibs.elasticsearch.qrcache.cache.QueryResultCache;
import org.codelibs.elasticsearch.qrcache.transport.QrcTransportInterceptor;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.watcher.ResourceWatcherService;

public class QueryResultCachePlugin extends Plugin implements ActionPlugin, NetworkPlugin {

    private Settings settings;

    private QueryResultCache queryResultCache;

    public QueryResultCachePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(QueryResultCache.CLEAN_INTERVAL_SETTING, //
                QueryResultCache.EXPIRE_SETTING, //
                QueryResultCache.MAX_SIZE_SETTING, //
                QueryResultCache.INDEX_ENABLED_SETTING);
    }

    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        return Arrays.asList(new QrcTransportInterceptor(queryResultCache));
    }

    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry,
            Environment environment, NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        queryResultCache = new QueryResultCache(settings, clusterService, threadPool);
        return Arrays.asList(queryResultCache);
    }
}
