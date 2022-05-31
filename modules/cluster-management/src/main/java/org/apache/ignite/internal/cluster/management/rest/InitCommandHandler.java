/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.cluster.management.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.rest.api.ErrorResult;
import org.apache.ignite.internal.rest.api.RequestHandler;
import org.apache.ignite.internal.rest.api.RestApiHttpRequest;
import org.apache.ignite.internal.rest.api.RestApiHttpResponse;
import org.apache.ignite.lang.IgniteLogger;

/**
 * REST handler for the {@link InitCommand}.
 */
public class InitCommandHandler implements RequestHandler {
    private static final IgniteLogger LOG = IgniteLogger.forClass(InitCommandHandler.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ClusterInitializer clusterInitializer;

    public InitCommandHandler(ClusterInitializer clusterInitializer) {
        this.clusterInitializer = clusterInitializer;
    }

    @Override
    public CompletableFuture<RestApiHttpResponse> handle(RestApiHttpRequest request, RestApiHttpResponse response) {
        try {
            InitCommand command = readContent(request);

            if (LOG.isInfoEnabled()) {
                LOG.info("Received init command:\n\t{}}", command);
            }

            return clusterInitializer.initCluster(command.metaStorageNodes(), command.cmgNodes(), command.clusterName())
                    .thenApply(v -> successResponse(response))
                    .exceptionally(e -> errorResponse(response, e));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(errorResponse(response, e));
        }
    }

    private InitCommand readContent(RestApiHttpRequest restApiHttpRequest) throws IOException {
        ByteBuf content = restApiHttpRequest.request().content();

        try (InputStream is = new ByteBufInputStream(content)) {
            return objectMapper.readValue(is, InitCommand.class);
        }
    }

    private static RestApiHttpResponse successResponse(RestApiHttpResponse response) {
        LOG.info("Init command executed successfully");

        return response;
    }

    private static RestApiHttpResponse errorResponse(RestApiHttpResponse response, Throwable e) {
        if (e instanceof CompletionException) {
            e = e.getCause();
        }

        LOG.error("Init command failure", e);

        response.status(INTERNAL_SERVER_ERROR);
        response.json(Map.of("error", new ErrorResult("APPLICATION_EXCEPTION", e.getMessage())));

        return response;
    }
}
