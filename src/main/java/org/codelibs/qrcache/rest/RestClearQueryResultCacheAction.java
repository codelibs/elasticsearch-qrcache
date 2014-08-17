package org.codelibs.qrcache.rest;

import java.io.IOException;

import org.codelibs.qrcache.cache.QueryResultCache;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;

public class RestClearQueryResultCacheAction extends BaseRestHandler {

    private QueryResultCache queryResultCache;

    @Inject
    public RestClearQueryResultCacheAction(final Settings settings,
            final Client client, final RestController controller,
            final QueryResultCache queryResultCache) {
        super(settings, client);
        this.queryResultCache = queryResultCache;

        controller.registerHandler(Method.POST, "/_qrc", this);
        controller.registerHandler(Method.POST, "/{index}/_qrc", this);
    }

    @Override
    public void handleRequest(final RestRequest request,
            final RestChannel channel, final Client client) {
        try {
            final String index = request.param("index");
            if (Strings.isEmpty(index)) {
                queryResultCache.clear();
            } else {
                final String[] indices = index.split(",");
                queryResultCache.clear(indices);
            }

            final XContentBuilder builder = JsonXContent.contentBuilder();
            final String pretty = request.param("pretty");
            if (pretty != null && !"false".equalsIgnoreCase(pretty)) {
                builder.prettyPrint().lfAtEnd();
            }
            builder.startObject();
            builder.field("acknowledged", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        } catch (final IOException e) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (final IOException e1) {
                logger.error("Failed to send a failure response.", e1);
            }
        }

    }

}
