package org.codelibs.elasticsearch.qrcache.rest;

import java.io.IOException;

import org.codelibs.elasticsearch.qrcache.QueryResultCachePlugin;
import org.codelibs.elasticsearch.qrcache.cache.QueryResultCache;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
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

public class RestClearQRCacheAction extends BaseRestHandler {

    protected final ESLogger logger;

    private QueryResultCache queryResultCache;

    @Inject
    public RestClearQRCacheAction(final Settings settings, final Client client,
            final RestController controller,
            final QueryResultCache queryResultCache) {
        super(settings, controller, client);
        this.queryResultCache = queryResultCache;
        this.logger = Loggers.getLogger(
                QueryResultCachePlugin.REST_LOGGER_NAME, settings);

        controller.registerHandler(Method.POST, "/_qrc/clear", this);
        controller.registerHandler(Method.POST, "/{index}/_qrc/clear", this);
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
