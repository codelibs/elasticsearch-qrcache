package org.codelibs.elasticsearch.qrcache.rest;

import java.io.IOException;

import org.codelibs.elasticsearch.qrcache.cache.QueryResultCache;
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

import com.google.common.base.Strings;

public class RestClearQrcAction extends BaseRestHandler {

    private QueryResultCache queryResultCache;

    public RestClearQrcAction(final Settings settings, final RestController controller, final QueryResultCache queryResultCache) {
        super(settings);
        this.queryResultCache = queryResultCache;

        controller.registerHandler(Method.POST, "/_qrc/clear", this);
        controller.registerHandler(Method.POST, "/{index}/_qrc/clear", this);
    }

    @Override
    public String getName() {
        return "qrcache_clear_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String index = request.param("index");
        final String pretty = request.param("pretty");

        return channel -> {
            if (Strings.isNullOrEmpty(index)) {
                queryResultCache.clear();
            } else {
                final String[] indices = index.split(",");
                queryResultCache.clear(indices);
            }

            final XContentBuilder builder = JsonXContent.contentBuilder();
            if (pretty != null && !"false".equalsIgnoreCase(pretty)) {
                builder.prettyPrint().lfAtEnd();
            }
            builder.startObject();
            builder.field("acknowledged", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };

    }

}
