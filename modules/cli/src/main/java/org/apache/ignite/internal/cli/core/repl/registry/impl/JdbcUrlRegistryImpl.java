package org.apache.ignite.internal.cli.core.repl.registry.impl;

import com.google.gson.Gson;
import jakarta.inject.Singleton;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.core.JdbcUrl;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionEventListener;
import org.apache.ignite.internal.cli.core.repl.config.RootConfig;
import org.apache.ignite.internal.cli.core.repl.registry.JdbcUrlRegistry;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;

@Singleton
public class JdbcUrlRegistryImpl implements JdbcUrlRegistry, SessionEventListener {

    private final IgniteLogger log = Loggers.forClass(getClass());
    private final NodeNameRegistryImpl nodeNameRegistry;
    private volatile Set<JdbcUrl> jdbcUrls = Set.of();
    private ScheduledExecutorService executor;

    public JdbcUrlRegistryImpl(NodeNameRegistryImpl nodeNameRegistry) {
        this.nodeNameRegistry = nodeNameRegistry;
    }

    private void fetchJdbcUrls() {
        jdbcUrls = nodeNameRegistry.urls()
                .stream()
                .map(URL::toString)
                .map(this::fetchJdbcUrl)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> jdbcUrls() {
        return jdbcUrls.stream()
                .map(JdbcUrl::toString)
                .collect(Collectors.toSet());
    }

    private JdbcUrl fetchJdbcUrl(String nodeUrl) {
        try {
            return constructJdbcUrl(fetchNodeConfiguration(nodeUrl), nodeUrl);
        } catch (Exception e) {
            log.warn("Couldn't fetch jdbc url of " + nodeUrl + " node: ", e);
            return null;
        }
    }

    private String fetchNodeConfiguration(String nodeUrl) throws ApiException {
        return new NodeConfigurationApi(Configuration.getDefaultApiClient().setBasePath(nodeUrl)).getNodeConfiguration();
    }

    private JdbcUrl constructJdbcUrl(String configuration, String nodeUrl) {
        try {
            int port = new Gson().fromJson(configuration, RootConfig.class).clientConnector.port;
            return JdbcUrl.of(nodeUrl, port);
        } catch (MalformedURLException ignored) {
            // Shouldn't happen ever since we are now connected to this URL
            return null;
        }
    }

    @Override
    public void onConnect(Session session) {
        if (executor == null) {
            executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("JdbcUrlRegistry", log));
            executor.scheduleWithFixedDelay(() -> fetchJdbcUrls(), 0, 5, TimeUnit.SECONDS);
        }
    }

    @Override
    public void onDisconnect() {

    }
}
