package org.codelibs.elasticsearch.qrcache.module;

import org.codelibs.elasticsearch.qrcache.cache.QueryResultCache;
import org.elasticsearch.common.inject.AbstractModule;

public class QueryResultCacheModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(QueryResultCache.class).asEagerSingleton();
    }
}