/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.rest;

import io.micronaut.context.ApplicationContext;
import io.micronaut.http.server.exceptions.ServerStartupException;
import io.micronaut.openapi.annotation.OpenAPIInclude;
import io.micronaut.runtime.Micronaut;
import io.micronaut.runtime.exceptions.ApplicationStartupException;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import java.net.BindException;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.rest.api.cluster.ClusterManagementApi;
import org.apache.ignite.internal.rest.api.cluster.TopologyApi;
import org.apache.ignite.internal.rest.api.configuration.ClusterConfigurationApi;
import org.apache.ignite.internal.rest.api.configuration.NodeConfigurationApi;
import org.apache.ignite.internal.rest.api.metric.NodeMetricApi;
import org.apache.ignite.internal.rest.api.node.NodeManagementApi;
import org.apache.ignite.internal.rest.configuration.RestConfiguration;
import org.apache.ignite.internal.rest.configuration.RestView;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Rest module is responsible for starting REST endpoints for accessing and managing configuration.
 *
 * <p>It is started on port 10300 by default but it is possible to change this in configuration itself. Refer to default config file in
 * resources for the example.
 */
@OpenAPIDefinition(info = @Info(
        title = "Ignite REST module",
        version = "3.0.0-SNAPSHOT",
        license = @License(name = "Apache 2.0", url = "https://ignite.apache.org"),
        contact = @Contact(email = "user@ignite.apache.org")))
@OpenAPIInclude(classes = {
        ClusterConfigurationApi.class,
        NodeConfigurationApi.class,
        ClusterManagementApi.class,
        NodeManagementApi.class,
        NodeMetricApi.class,
        TopologyApi.class
})
public class RestComponent implements IgniteComponent {
    /** Default port. */
    public static final int DFLT_PORT = 10300;

    /** Server host. */
    private static final String LOCALHOST = "localhost";

    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RestComponent.class);

    /** Factories that produce beans needed for REST controllers. */
    private final List<RestFactory> restFactories;

    private final RestConfiguration restConfiguration;

    /** Micronaut application context. */
    private volatile ApplicationContext context;

    /** Server port. */
    private int port;

    /**
     * Creates a new instance of REST module.
     */
    public RestComponent(List<RestFactory> restFactories, RestConfiguration restConfiguration) {
        this.restFactories = restFactories;
        this.restConfiguration = restConfiguration;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        RestView restConfigurationView = restConfiguration.value();

        int desiredPort = restConfigurationView.port();
        int portRange = restConfigurationView.portRange();

        for (int portCandidate = desiredPort; portCandidate <= desiredPort + portRange; portCandidate++) {
            try {
                port = portCandidate;
                context = buildMicronautContext(portCandidate).start();
                LOG.info("REST protocol started successfully");
                return;
            } catch (ApplicationStartupException e) {
                BindException bindException = findBindException(e);
                if (bindException != null) {
                    LOG.debug("Got exception during node start, going to try again [port={}]", portCandidate);
                    continue;
                }
                throw new RuntimeException(e);
            }
        }

        LOG.debug("Unable to start REST endpoint. All ports are in use [ports=[{}, {}]]", desiredPort, (desiredPort + portRange));

        String msg = "Cannot start REST endpoint. " + "All ports in range [" + desiredPort + ", " + (desiredPort + portRange)
                + "] are in use.";

        throw new RuntimeException(msg);
    }

    @Nullable
    private BindException findBindException(ApplicationStartupException e) {
        var throwable = e.getCause();
        while (throwable != null) {
            if (throwable instanceof BindException) {
                return (BindException) throwable;
            }
            throwable = throwable.getCause();
        }
        return null;
    }

    private Micronaut buildMicronautContext(int portCandidate) {
        Micronaut micronaut = Micronaut.build("");
        setFactories(micronaut);
        return micronaut
                .properties(Map.of("micronaut.server.port", portCandidate))
                .banner(false)
                .mapError(ServerStartupException.class, this::mapServerStartupException)
                .mapError(ApplicationStartupException.class, ex -> -1);
    }

    private void setFactories(Micronaut micronaut) {
        for (var factory : restFactories) {
            micronaut.singletons(factory);
        }
    }

    private int mapServerStartupException(ServerStartupException exception) {
        if (exception.getCause() instanceof BindException) {
            return -1; // -1 forces the micronaut to throw an ApplicationStartupException
        } else {
            return 1;
        }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void stop() throws Exception {
        // TODO: IGNITE-16636 Use busy-lock approach to guard stopping RestComponent
        if (context != null) {
            context.stop();
            context = null;
        }
    }

    /**
     * Returns server port.
     *
     * @throws IgniteInternalException if the component has not been started yet.
     */
    public int port() {
        if (context == null) {
            throw new IgniteInternalException("RestComponent has not been started");
        }

        return port;
    }

    /**
     * Returns server host.
     *
     * @throws IgniteInternalException if the component has not been started yet.
     */
    public String host() {
        if (context == null) {
            throw new IgniteInternalException("RestComponent has not been started");
        }

        return LOCALHOST;
    }
}
