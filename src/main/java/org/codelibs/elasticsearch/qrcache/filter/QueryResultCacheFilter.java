package org.codelibs.elasticsearch.qrcache.filter;

import org.codelibs.elasticsearch.qrcache.cache.QueryResultCache;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class QueryResultCacheFilter implements ActionFilter {
    private int order;

    private QueryResultCache queryResultCache;

    private ThreadLocal<SearchType> currentSearchType = new ThreadLocal<>();

    @Inject
    public QueryResultCacheFilter(final Settings settings,
            final QueryResultCache queryResultCache) {
        this.queryResultCache = queryResultCache;

        order = settings.getAsInt("indices.qrc.filter.order", 5);
    }

    @Override
    public int order() {
        return order;
    }

    @Override
    public void apply(final String action, final ActionRequest request,
            final ActionListener listener, final ActionFilterChain chain) {
        if (!SearchAction.INSTANCE.name().equals(action)) {
            chain.proceed(action, request, listener);
            return;
        }

        final SearchRequest searchRequest = (SearchRequest) request;
        final SearchType searchType = currentSearchType.get();
        if (searchType == null) {
            try {
                currentSearchType.set(searchRequest.searchType());
                chain.proceed(action, request, listener);
            } finally {
                currentSearchType.remove();
            }
        } else if (queryResultCache.canCache(searchRequest)) {
            final ActionListener cacheListener = queryResultCache.execute(
                    searchRequest, listener, chain);
            if (cacheListener != null) {
                chain.proceed(action, request, cacheListener);
            }
        } else {
            chain.proceed(action, request, listener);
        }

    }

    @Override
    public void apply(final String action, final ActionResponse response,
            final ActionListener listener, final ActionFilterChain chain) {
        chain.proceed(action, response, listener);
    }

}
