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

import static io.micronaut.context.env.Environment.BARE_METAL;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import io.micronaut.context.ApplicationContext;
import io.micronaut.http.server.exceptions.ServerStartupException;
import io.micronaut.http.ssl.ClientAuthentication;
import io.micronaut.runtime.Micronaut;
import io.micronaut.runtime.exceptions.ApplicationStartupException;
import io.netty.handler.ssl.ClientAuth;
import java.net.BindException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.configuration.KeyStoreView;
import org.apache.ignite.internal.rest.configuration.RestConfiguration;
import org.apache.ignite.internal.rest.configuration.RestSslView;
import org.apache.ignite.internal.rest.configuration.RestView;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Rest module is responsible for starting REST endpoints for accessing and managing configuration.
 *
 * <p>It is started on port 10300 by default but it is possible to change this in configuration itself. Refer to default config file in
 * resources for the example.
 */
public class RestComponent implements IgniteComponent {
    /**
     * Lock for micronaut server startup.
     * TODO: remove when fix https://github.com/micronaut-projects/micronaut-core/issues/10091
     */
    private static final Object SHARED_STARTUP_LOCK = new Object();

    /** Unavailable port. */
    private static final int UNAVAILABLE_PORT = -1;

    /** Server host. */
    private static final String LOCALHOST = "localhost";

    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RestComponent.class);

    /** Factories that produce beans needed for REST controllers. */
    private final List<Supplier<RestFactory>> restFactories;

    /** Rest configuration. */
    private final RestConfiguration restConfiguration;

    /** Rest manager. */
    private final RestManager restManager;

    /** Micronaut application context. */
    private volatile ApplicationContext context;

    /** Server port. */
    private volatile int httpPort = UNAVAILABLE_PORT;

    /** Server SSL port. */
    private volatile int httpsPort = UNAVAILABLE_PORT;

    /**
     * Creates a new instance of REST module.
     */
    public RestComponent(
            List<Supplier<RestFactory>> restFactories,
            RestManager restManager,
            RestConfiguration restConfiguration
    ) {
        this.restFactories = restFactories;
        this.restConfiguration = restConfiguration;
        this.restManager = restManager;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync(ExecutorService startupExecutor) {
        RestView restConfigurationView = restConfiguration.value();
        RestSslView sslConfigurationView = restConfigurationView.ssl();

        boolean sslEnabled = sslConfigurationView.enabled();
        boolean dualProtocol = restConfiguration.dualProtocol().value();

        if (startServer(restConfigurationView.port(), sslConfigurationView.port(), sslEnabled, dualProtocol)) {
            return nullCompletedFuture();
        }

        String msg = "Cannot start REST endpoint."
                + " Couldn't find available port for HTTP or HTTPS"
                + " [HTTP port=" + httpPort + "],"
                + " [HTTPS port=" + httpsPort + "]";

        LOG.error(msg);

        throw new IgniteException(Common.COMPONENT_NOT_STARTED_ERR, msg);
    }

    /**
     * Disable REST component.
     */
    public void disable() {
        restManager.setState(RestState.INITIALIZATION);
    }

    /**
     * Enable REST component.
     */
    public void enable() {
        restManager.setState(RestState.INITIALIZED);
    }

    /** Starts Micronaut application using the provided ports.
     *
     * @param httpPortCandidate HTTP port candidate.
     * @param httpsPortCandidate HTTPS port candidate.
     * @param sslEnabled SSL enabled flag.
     * @param dualProtocol Dual protocol flag.
     * @return {@code True} if server was started successfully, {@code False} if couldn't bind one of the ports.
     */
    private boolean startServer(int httpPortCandidate, int httpsPortCandidate, boolean sslEnabled, boolean dualProtocol) {
        // Workaround to avoid micronaut race condition on startup.
        synchronized (SHARED_STARTUP_LOCK) {
            try {
                httpPort = httpPortCandidate;
                httpsPort = httpsPortCandidate;

                context = buildMicronautContext(httpPortCandidate, httpsPortCandidate)
                        .deduceEnvironment(false)
                        .environments(BARE_METAL)
                        .start();

                logSuccessRestStart(sslEnabled, dualProtocol);
                return true;
            } catch (ApplicationStartupException e) {
                BindException bindException = findBindException(e);
                if (bindException != null) {
                    return false;
                }
                throw new IgniteException(Common.COMPONENT_NOT_STARTED_ERR, e);
            }
        }
    }

    private void logSuccessRestStart(boolean sslEnabled, boolean dualProtocol) {
        String successReportMsg;

        if (sslEnabled && dualProtocol) {
            successReportMsg = "[httpPort=" + httpPort + "], [httpsPort=" + httpsPort + "]";
        } else if (sslEnabled) {
            successReportMsg = "[httpsPort=" + httpsPort + "]";
        } else {
            successReportMsg = "[httpPort=" + httpPort + "]";
        }

        LOG.info("REST server started successfully: " + successReportMsg);
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

    private Micronaut buildMicronautContext(int portCandidate, int sslPortCandidate) {
        Micronaut micronaut = Micronaut.build("");
        setFactories(micronaut);

        return micronaut
                .properties(serverProperties(portCandidate, sslPortCandidate))
                .banner(false)
                // -1 forces the micronaut to throw an ApplicationStartupException instead of doing System.exit
                .mapError(ServerStartupException.class, ex -> -1)
                .mapError(ApplicationStartupException.class, ex -> -1);
    }

    private void setFactories(Micronaut micronaut) {
        for (var factory : restFactories) {
            micronaut.singletons(factory.get());
        }
    }

    private Map<String, Object> serverProperties(int port, int sslPort) {
        RestSslView restSslView = restConfiguration.ssl().value();
        boolean sslEnabled = restSslView.enabled();

        Map<String, Object> result = new HashMap<>();
        result.put("micronaut.server.port", port);
        result.put("micronaut.server.cors.enabled", "true");
        result.put("micronaut.server.cors.configurations.web.allowed-headers", "Authorization");
        result.put("micronaut.security.intercept-url-map[0].pattern", "/**");
        result.put("micronaut.security.intercept-url-map[0].access", "isAuthenticated()");

        if (sslEnabled) {
            KeyStoreView keyStore = restSslView.keyStore();
            boolean dualProtocol = restConfiguration.dualProtocol().value();

            // Micronaut is not going to handle requests on that port, but it's required
            result.put("micronaut.server.dual-protocol", dualProtocol);
            result.put("micronaut.server.ssl.port", sslPort);
            if (!restSslView.ciphers().isBlank()) {
                result.put("micronaut.server.ssl.ciphers", restSslView.ciphers());
            }
            result.put("micronaut.server.ssl.enabled", sslEnabled);
            result.put("micronaut.server.ssl.key-store.path", "file:" + keyStore.path());
            result.put("micronaut.server.ssl.key-store.password", keyStore.password());
            result.put("micronaut.server.ssl.key-store.type", keyStore.type());

            ClientAuth clientAuth = ClientAuth.valueOf(restSslView.clientAuth().toUpperCase());
            if (ClientAuth.NONE == clientAuth) {
                return result;
            }

            KeyStoreView trustStore = restSslView.trustStore();

            result.put("micronaut.server.ssl.client-authentication", toMicronautClientAuth(clientAuth));
            result.put("micronaut.server.ssl.trust-store.path", "file:" + trustStore.path());
            result.put("micronaut.server.ssl.trust-store.password", trustStore.password());
            result.put("micronaut.server.ssl.trust-store.type", trustStore.type());
        }

        return result;
    }

    private static String toMicronautClientAuth(ClientAuth clientAuth) {
        switch (clientAuth) {
            case OPTIONAL: return ClientAuthentication.WANT.name().toLowerCase();
            case REQUIRE:  return ClientAuthentication.NEED.name().toLowerCase();
            default: throw new IllegalArgumentException("Can not convert " + clientAuth.name() + " to micronaut type");
        }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized CompletableFuture<Void> stopAsync(ExecutorService stopExecutor) {
        // TODO: IGNITE-16636 Use busy-lock approach to guard stopping RestComponent
        if (context != null) {
            context.stop();
            context = null;
        }

        return nullCompletedFuture();
    }

    /**
     * Returns server port.
     *
     * @return server port or -1 if HTTP is unavailable.
     * @throws IgniteInternalException if the component has not been started yet.
     */
    public int httpPort() {
        RestView restView = restConfiguration.value();
        if (!restView.ssl().enabled() || restView.dualProtocol()) {
            return httpPort;
        } else {
            return UNAVAILABLE_PORT;
        }
    }

    /**
     * Returns server SSL port.
     *
     * @return server SSL port or -1 if HTTPS is unavailable.
     * @throws IgniteInternalException if the component has not been started yet.
     */
    public int httpsPort() {
        RestView restView = restConfiguration.value();
        if (restView.ssl().enabled()) {
            return httpsPort;
        } else {
            return UNAVAILABLE_PORT;
        }
    }

    /**
     * Returns server host name.
     *
     * @return host.
     * @throws IgniteInternalException if the component has not been started yet.
     */
    public String hostName() {
        if (context == null) {
            throw new IgniteInternalException("RestComponent has not been started");
        }

        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return LOCALHOST;
        }
    }
}
