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

    /** Unavailable port. */
    private static final int UNAVAILABLE_PORT = -1;

    /** Server host. */
    private static final String LOCALHOST = "localhost";

    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RestComponent.class);

    /** Factories that produce beans needed for REST controllers. */
    private final List<Supplier<RestFactory>> restFactories;

    private final RestConfiguration restConfiguration;

    /** Micronaut application context. */
    private volatile ApplicationContext context;

    /** Server port. */
    private int httpPort = UNAVAILABLE_PORT;

    /** Server SSL port. */
    private int httpsPort = UNAVAILABLE_PORT;

    /**
     * Creates a new instance of REST module.
     */
    public RestComponent(List<Supplier<RestFactory>> restFactories, RestConfiguration restConfiguration) {
        this.restFactories = restFactories;
        this.restConfiguration = restConfiguration;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        RestView restConfigurationView = restConfiguration.value();
        RestSslView sslConfigurationView = restConfigurationView.ssl();

        boolean sslEnabled = sslConfigurationView.enabled();
        boolean dualProtocol = restConfiguration.dualProtocol().value();
        int desiredHttpPort = restConfigurationView.port();
        int portRange = restConfigurationView.portRange();
        int desiredHttpsPort = sslConfigurationView.port();
        int httpsPortRange = sslConfigurationView.portRange();
        int httpPortCandidate = desiredHttpPort;
        int httpsPortCandidate = desiredHttpsPort;

        while (httpPortCandidate <= desiredHttpPort + portRange
                && httpsPortCandidate <= desiredHttpsPort + httpsPortRange) {
            if (startServer(httpPortCandidate, httpsPortCandidate)) {
                return;
            }

            LOG.debug("Got exception during node start, going to try again [httpPort={}, httpsPort={}]",
                    httpPortCandidate,
                    httpsPortCandidate);

            if (sslEnabled && dualProtocol) {
                httpPortCandidate++;
                httpsPortCandidate++;
            } else if (sslEnabled) {
                httpsPortCandidate++;
            } else {
                httpPortCandidate++;
            }
        }

        LOG.debug("Unable to start REST endpoint."
                        + " Couldn't find available port for HTTP or HTTPS"
                        + " [HTTP ports=[{}, {}]],"
                        + " [HTTPS ports=[{}, {}]]",
                desiredHttpPort, (desiredHttpPort + portRange),
                desiredHttpsPort, (desiredHttpsPort + httpsPortRange));

        String msg = "Cannot start REST endpoint."
                + " Couldn't find available port for HTTP or HTTPS"
                + " [HTTP ports=[" + desiredHttpPort + ", " + desiredHttpPort + portRange + "]],"
                + " [HTTPS ports=[" + desiredHttpsPort + ", " + desiredHttpsPort + httpsPortRange + "]]";

        throw new IgniteException(Common.COMPONENT_NOT_STARTED_ERR, msg);
    }

    /** Starts Micronaut application using the provided ports.
     *
     * @param httpPortCandidate HTTP port candidate.
     * @param httpsPortCandidate HTTPS port candidate.
     * @return {@code True} if server was started successfully, {@code False} if couldn't bind one of the ports.
     */
    private boolean startServer(int httpPortCandidate, int httpsPortCandidate) {
        try {
            httpPort = httpPortCandidate;
            httpsPort = httpsPortCandidate;
            context = buildMicronautContext(httpPortCandidate, httpsPortCandidate)
                    .deduceEnvironment(false)
                    .environments(BARE_METAL)
                    .start();
            LOG.info("REST protocol started successfully");
            return true;
        } catch (ApplicationStartupException e) {
            BindException bindException = findBindException(e);
            if (bindException != null) {
                return false;
            }
            throw new IgniteException(Common.COMPONENT_NOT_STARTED_ERR, e);
        }
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

        Map<String, Object> properties = new HashMap<>();
        properties.putAll(serverProperties(portCandidate, sslPortCandidate));
        properties.putAll(authProperties());

        return micronaut
                .properties(properties)
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
        result.put("ignite.endpoints.filter-non-initialized", "true");

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

    private Map<String, Object> authProperties() {
        return Map.of("micronaut.security.enabled", true,
                        "micronaut.security.intercept-url-map[1].pattern", "/**",
                        "micronaut.security.intercept-url-map[1].access", "isAuthenticated()");
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
     * Returns server host.
     *
     * @return host.
     * @throws IgniteInternalException if the component has not been started yet.
     */
    public String host() {
        if (context == null) {
            throw new IgniteInternalException("RestComponent has not been started");
        }

        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return LOCALHOST;
        }
    }
}
