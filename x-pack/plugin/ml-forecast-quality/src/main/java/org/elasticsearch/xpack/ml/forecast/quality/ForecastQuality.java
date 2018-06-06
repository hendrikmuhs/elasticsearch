/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.forecast.quality;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.ml.forecast.quality.action.ForecastEvaluateAction;
import org.elasticsearch.xpack.ml.forecast.quality.action.TransportForecastEvaluateAction;
import org.elasticsearch.xpack.ml.forecast.quality.rest.action.RestForecastEvaluateAction;

import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

public class ForecastQuality extends Plugin implements ActionPlugin {

    public static final String NAME = "forecast_quality";
    protected final boolean enabled;

    public ForecastQuality(Settings settings) {
        this.enabled = true;
    }

    @Override
    public List<RestHandler> getRestHandlers(final Settings settings, final RestController restController,
            final ClusterSettings clusterSettings, final IndexScopedSettings indexScopedSettings, final SettingsFilter settingsFilter,
            final IndexNameExpressionResolver indexNameExpressionResolver, final Supplier<DiscoveryNodes> nodesInCluster) {

        return singletonList(new RestForecastEvaluateAction(settings, restController));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {

        return singletonList(new ActionHandler<>(ForecastEvaluateAction.INSTANCE, TransportForecastEvaluateAction.class));
    }
}
