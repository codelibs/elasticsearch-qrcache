package org.codelibs.elasticsearch.qrcache;

import java.util.Collection;

import org.codelibs.elasticsearch.extension.module.ExtensionModule;
import org.codelibs.elasticsearch.qrcache.filter.QueryResultCacheFilter;
import org.codelibs.elasticsearch.qrcache.filter.RefreshEngineFilter;
import org.codelibs.elasticsearch.qrcache.module.QueryResultCacheModule;
import org.codelibs.elasticsearch.qrcache.rest.RestClearQRCacheAction;
import org.codelibs.elasticsearch.qrcache.rest.RestStatsQRCacheAction;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.settings.IndexDynamicSettingsModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

public class QueryResultCachePlugin extends AbstractPlugin {
    public static final String REST_LOGGER_NAME = "rest.action.admin.qrcache";

    public static final String INDEX_LOGGER_NAME = "index.qrcache";

    @Override
    public String name() {
        return "QueryResultCachePlugin";
    }

    @Override
    public String description() {
        return "This is Query Result Cache plugin.";
    }

    public void onModule(final ActionModule module) {
        module.registerFilter(QueryResultCacheFilter.class);
    }

    public void onModule(final ExtensionModule module) {
        module.registerEngineFilter(RefreshEngineFilter.class);
    }

    public void onModule(final RestModule module) {
        module.addRestAction(RestClearQRCacheAction.class);
        module.addRestAction(RestStatsQRCacheAction.class);
    }

    public void onModule(final IndexDynamicSettingsModule module) {
        module.addDynamicSettings("index.cache.query_result.*");
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists
                .newArrayList();
        modules.add(QueryResultCacheModule.class);
        return modules;
    }

}
