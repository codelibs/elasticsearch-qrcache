package org.codelibs.elasticsearch.qrcache.filter;

import org.codelibs.elasticsearch.qrcache.cache.QueryResultCache;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class QueryResultCacheFilter implements ActionFilter {
    private static final String SEARCH_REQUEST_INVOKED = "filter.codelibs.qrcache.Invoked";

    private int order;

    private QueryResultCache queryResultCache;

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
    public void apply(final String action,
            @SuppressWarnings("rawtypes") final ActionRequest request,
            @SuppressWarnings("rawtypes") final ActionListener listener,
            final ActionFilterChain chain) {
        if (!SearchAction.INSTANCE.name().equals(action)) {
            chain.proceed(action, request, listener);
            return;
        }

        final SearchRequest searchRequest = (SearchRequest) request;
        final Boolean invoked = searchRequest.getHeader(SEARCH_REQUEST_INVOKED);
        if (invoked != null && invoked.booleanValue()) {
            if (queryResultCache.canCache(searchRequest)) {
                @SuppressWarnings({ "rawtypes", "unchecked" })
                final ActionListener cacheListener = queryResultCache
                        .execute(searchRequest, listener, chain);
                if (cacheListener != null) {
                    chain.proceed(action, request, cacheListener);
                }
            } else {
                chain.proceed(action, request, listener);
            }
        } else {
            searchRequest.putHeader(SEARCH_REQUEST_INVOKED, Boolean.TRUE);
            chain.proceed(action, request, listener);
        }
    }

    @Override
    public void apply(final String action, final ActionResponse response,
            @SuppressWarnings("rawtypes") final ActionListener listener, final ActionFilterChain chain) {
        chain.proceed(action, response, listener);
    }

}
