package org.codelibs.qrcache;

import java.util.Collection;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

import org.codelibs.qrcache.module.QueryResultCacheModule;
import org.codelibs.qrcache.rest.RestClearQRCacheAction;
import org.codelibs.qrcache.rest.RestStatsQRCacheAction;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.settings.IndexDynamicSettingsModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

public class QueryResultCachePlugin extends AbstractPlugin {

    public QueryResultCachePlugin() throws Exception {
        updateTransportSearchAction();
        updateInternalEngine();
    }

    private void updateTransportSearchAction() throws Exception {
        final String transportSearchActionClsName = "org.elasticsearch.action.search.TransportSearchAction";
        final String queryResultCacheClsName = "org.codelibs.qrcache.cache.QueryResultCache";

        final ClassPool classPool = ClassPool.getDefault();
        final CtClass cc = classPool.get(transportSearchActionClsName);

        final CtMethod createAndPutContextMethod = cc
                .getDeclaredMethod(
                        "doExecute",
                        new CtClass[] {
                                classPool
                                        .get("org.elasticsearch.action.search.SearchRequest"),
                                classPool
                                        .get("org.elasticsearch.action.ActionListener") });
        createAndPutContextMethod.insertBefore(//
                "if(" + queryResultCacheClsName + ".get().begin()){"//
                        + queryResultCacheClsName + ".get().execute($0,$1,$2);"//
                        + "return;"//
                        + "}"//
                );
        createAndPutContextMethod.insertAfter(queryResultCacheClsName
                + ".get().end();", true);

        final ClassLoader classLoader = this.getClass().getClassLoader();
        cc.toClass(classLoader, this.getClass().getProtectionDomain());
    }

    private void updateInternalEngine() throws Exception {
        final String internalEngineClsName = "org.elasticsearch.index.engine.internal.InternalEngine";
        final String queryResultCacheClsName = "org.codelibs.qrcache.cache.QueryResultCache";

        final ClassPool classPool = ClassPool.getDefault();
        final CtClass cc = classPool.get(internalEngineClsName);

        final CtMethod refreshMethod = cc
                .getDeclaredMethod("refresh", new CtClass[] { classPool
                        .get("org.elasticsearch.index.engine.Engine$Refresh") });
        refreshMethod.insertAfter(queryResultCacheClsName
                + ".get().clear(shardId.getIndex());");

        final ClassLoader classLoader = this.getClass().getClassLoader();
        cc.toClass(classLoader, this.getClass().getProtectionDomain());
    }

    @Override
    public String name() {
        return "QueryResultCachePlugin";
    }

    @Override
    public String description() {
        return "This is Query Result Cache plugin.";
    }

    // for Rest API
    public void onModule(final RestModule module) {
        module.addRestAction(RestClearQRCacheAction.class);
        module.addRestAction(RestStatsQRCacheAction.class);
    }

    public void onModule(final IndexDynamicSettingsModule module) {
        module.addDynamicSettings("index.cache.query_result.enable");
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists
                .newArrayList();
        modules.add(QueryResultCacheModule.class);
        return modules;
    }

}
