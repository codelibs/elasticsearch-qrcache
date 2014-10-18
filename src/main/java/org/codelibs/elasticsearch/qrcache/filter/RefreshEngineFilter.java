package org.codelibs.elasticsearch.qrcache.filter;

import org.codelibs.elasticsearch.extension.chain.EngineChain;
import org.codelibs.elasticsearch.extension.filter.BaseEngineFilter;
import org.codelibs.elasticsearch.qrcache.cache.QueryResultCache;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.Engine.Refresh;
import org.elasticsearch.index.engine.EngineException;

public class RefreshEngineFilter extends BaseEngineFilter {

    private QueryResultCache queryResultCache;

    @Inject
    public RefreshEngineFilter(final QueryResultCache queryResultCache) {
        this.queryResultCache = queryResultCache;
    }

    @Override
    public int order() {
        return 100;
    }

    @Override
    public void doRefresh(final Refresh refresh, final EngineChain chain)
            throws EngineException {
        chain.doRefresh(refresh);
        queryResultCache.clear(chain.getEngine().shardId().getIndex());
    }

}
