package org.codelibs.elasticsearch.qrcache.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.qrcache.cache.QueryResultCache;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

public class QrcTransportInterceptor implements TransportInterceptor {
    private static final Logger logger = LogManager.getLogger(QrcTransportInterceptor.class);

    private QueryResultCache queryResultCache;

    public QrcTransportInterceptor(QueryResultCache queryResultCache) {
        this.queryResultCache = queryResultCache;
    }

    @Override
    public AsyncSender interceptSender(AsyncSender sender) {
        return new QrcAsyncSender(sender);
    }

    class QrcAsyncSender implements AsyncSender {

        private AsyncSender sender;

        public QrcAsyncSender(AsyncSender sender) {
            this.sender = sender;
        }

        @Override
        public <T extends TransportResponse> void sendRequest(final Connection connection, final String action,
                final TransportRequest request, final TransportRequestOptions options, final TransportResponseHandler<T> handler) {
            if (SearchTransportService.QUERY_ACTION_NAME.equals(action)) {
                ShardSearchTransportRequest req = (ShardSearchTransportRequest) request;
                if (queryResultCache.canCache(req)) {
                    queryResultCache.sendCacheRequest(connection, action,req, options, handler, sender);
                    return;
                }
            }
            sender.sendRequest(connection, action, request, options, handler);
        }

    }
}
