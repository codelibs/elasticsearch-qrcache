package org.codelibs.qrcache.module;

import org.codelibs.qrcache.cache.QueryResultCache;
import org.elasticsearch.common.inject.AbstractModule;

public class QueryResultCacheModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(QueryResultCache.class).asEagerSingleton();
    }
}