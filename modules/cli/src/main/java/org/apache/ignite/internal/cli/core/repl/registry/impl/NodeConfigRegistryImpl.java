package org.apache.ignite.internal.cli.core.repl.registry.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.inject.Singleton;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cli.call.configuration.NodeConfigShowCall;
import org.apache.ignite.internal.cli.call.configuration.NodeConfigShowCallInput;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionEventListener;
import org.apache.ignite.internal.cli.core.repl.registry.NodeConfigRegistry;

@Singleton
public class NodeConfigRegistryImpl implements NodeConfigRegistry, SessionEventListener {
    private final NodeConfigShowCall nodeConfigShowCall;

    private final AtomicReference<Config> config = new AtomicReference<>(null);

    public NodeConfigRegistryImpl(NodeConfigShowCall nodeConfigShowCall) {
        this.nodeConfigShowCall = nodeConfigShowCall;
    }

    @Override
    public void onConnect(Session session) {
        CompletableFuture.runAsync(() -> {
            try {
                config.set(ConfigFactory.parseString(
                        nodeConfigShowCall.execute(
                                // todo https://issues.apache.org/jira/browse/IGNITE-17416
                                NodeConfigShowCallInput.builder().nodeUrl(session.sessionDetails().nodeUrl()).build()
                        ).body().getValue())
                );
            } catch (Exception ignored) {
                // no-op
            }
        });
    }

    @Override
    public void onDisconnect() {

    }

    /** {@inheritDoc} */
    @Override
    public Config config() {
        return this.config.get();
    }
}
