/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.configuration.rest;

import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.rest.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.configuration.rest.presentation.hocon.HoconPresentation;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.rest.ErrorResult;
import org.apache.ignite.rest.RestApiHttpRequest;
import org.apache.ignite.rest.RestApiHttpResponse;
import org.apache.ignite.rest.RestHandlersRegister;
import org.apache.ignite.rest.Routes;

/**
 * HTTP handlers for configuration-related REST endpoints.
 */
public class ConfigurationHttpHandlers {
    /**
     * Node configuration route.
     */
    private static final String NODE_CFG_URL = "/management/v1/configuration/node/";

    /**
     * Cluster configuration route.
     */
    private static final String CLUSTER_CFG_URL = "/management/v1/configuration/cluster/";

    /**
     * Path parameter.
     */
    private static final String PATH_PARAM = "selector";

    private final ConfigurationManager nodeConfigurationManager;
    private final ConfigurationManager clusterConfigurationManager;

    public ConfigurationHttpHandlers(ConfigurationManager nodeConfigurationManager, ConfigurationManager clusterConfigurationManager) {
        this.nodeConfigurationManager = nodeConfigurationManager;
        this.clusterConfigurationManager = clusterConfigurationManager;
    }

    public void registerHandlers(RestHandlersRegister register) {
        register.registerHandlers(this::registerOn);
    }

    private void registerOn(Routes routes) {
        var nodeCfgPresentation = new HoconPresentation(nodeConfigurationManager.configurationRegistry());
        var clusterCfgPresentation = new HoconPresentation(clusterConfigurationManager.configurationRegistry());

        routes
                .get(
                        NODE_CFG_URL,
                        (req, resp) -> {
                            resp.json(nodeCfgPresentation.represent());

                            return CompletableFuture.completedFuture(resp);
                        }
                )
                .get(
                        CLUSTER_CFG_URL,
                        (req, resp) -> {
                            resp.json(clusterCfgPresentation.represent());

                            return CompletableFuture.completedFuture(resp);
                        }
                )
                .get(
                        NODE_CFG_URL + ":" + PATH_PARAM,
                        (req, resp) -> handleRepresentByPath(req, resp, nodeCfgPresentation)
                )
                .get(
                        CLUSTER_CFG_URL + ":" + PATH_PARAM,
                        (req, resp) -> handleRepresentByPath(req, resp, clusterCfgPresentation)
                )
                .patch(
                        NODE_CFG_URL,
                        APPLICATION_JSON.toString(),
                        (req, resp) -> handleUpdate(req, resp, nodeCfgPresentation)
                )
                .patch(
                        CLUSTER_CFG_URL,
                        APPLICATION_JSON.toString(),
                        (req, resp) -> handleUpdate(req, resp, clusterCfgPresentation)
                );
    }

    /**
     * Handle a request to get the configuration by {@link #PATH_PARAM path}.
     *
     * @param req          Rest request.
     * @param res          Rest response.
     * @param presentation Configuration presentation.
     */
    private static CompletableFuture<RestApiHttpResponse> handleRepresentByPath(
            RestApiHttpRequest req,
            RestApiHttpResponse res,
            ConfigurationPresentation<String> presentation
    ) {
        try {
            String cfgPath = req.queryParams().get(PATH_PARAM);

            res.json(presentation.representByPath(cfgPath));
        } catch (IllegalArgumentException pathE) {
            ErrorResult errRes = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", pathE.getMessage());

            res.status(BAD_REQUEST);
            res.json(Map.of("error", errRes));
        }

        return CompletableFuture.completedFuture(res);
    }

    /**
     * Handle a configuration update request as json.
     *
     * @param req          Rest request.
     * @param res          Rest response.
     * @param presentation Configuration presentation.
     */
    private static CompletableFuture<RestApiHttpResponse> handleUpdate(
            RestApiHttpRequest req,
            RestApiHttpResponse res,
            ConfigurationPresentation<String> presentation
    ) {
        String updateReq = req.request().content().toString(StandardCharsets.UTF_8);

        return presentation.update(updateReq)
                .thenApply(v -> res)
                .exceptionally(e -> {
                    if (e instanceof CompletionException) {
                        e = e.getCause();
                    }

                    ErrorResult errRes;

                    if (e instanceof IllegalArgumentException) {
                        errRes = new ErrorResult("INVALID_CONFIG_FORMAT", e.getMessage());
                    } else if (e instanceof ConfigurationValidationException) {
                        errRes = new ErrorResult("VALIDATION_EXCEPTION", e.getMessage());
                    } else if (e instanceof IgniteException) {
                        errRes = new ErrorResult("APPLICATION_EXCEPTION", e.getMessage());
                    } else {
                        throw new CompletionException(e);
                    }

                    res.status(BAD_REQUEST);
                    res.json(Map.of("error", errRes));

                    return res;
                });
    }
}
