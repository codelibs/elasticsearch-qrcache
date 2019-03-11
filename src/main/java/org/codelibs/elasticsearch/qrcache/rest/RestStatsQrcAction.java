package org.codelibs.elasticsearch.qrcache.rest;

import java.io.IOException;

import org.codelibs.elasticsearch.qrcache.cache.QueryResultCache;
import org.codelibs.elasticsearch.qrcache.cache.QueryResultCacheStats;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;

public class RestStatsQrcAction extends BaseRestHandler {

    private QueryResultCache queryResultCache;

    public RestStatsQrcAction(final Settings settings, final RestController controller, final QueryResultCache queryResultCache) {
        super(settings);
        this.queryResultCache = queryResultCache;

        controller.registerHandler(Method.GET, "/_qrc/stats", this);
    }

    @Override
    public String getName() {
        return "qrcache_stats_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String pretty = request.param("pretty");

        return channel -> {
            final QueryResultCacheStats stats = queryResultCache.stats();

            final XContentBuilder builder = JsonXContent.contentBuilder();
            if (pretty != null && !"false".equalsIgnoreCase(pretty)) {
                builder.prettyPrint().lfAtEnd();
            }
            builder.startObject();
            builder.startObject("_all");
            stats.toXContent(builder, null);
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }

}
