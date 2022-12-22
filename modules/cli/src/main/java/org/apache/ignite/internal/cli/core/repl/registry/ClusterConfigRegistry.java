package org.apache.ignite.internal.cli.core.repl.registry;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.inject.Singleton;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionEventListener;

@Singleton
public class ClusterConfigRegistry implements SessionEventListener {

    private final ClusterConfigShowCall clusterConfigShowCall;

    private final AtomicReference<Config> config = new AtomicReference<>(null);

    public ClusterConfigRegistry(ClusterConfigShowCall clusterConfigShowCall) {
        this.clusterConfigShowCall = clusterConfigShowCall;
    }

    @Override
    public void onConnect(Session session) {
        CompletableFuture.runAsync(() -> {
            try {
                config.set(ConfigFactory.parseString(
                        clusterConfigShowCall.execute(
                                // todo https://issues.apache.org/jira/browse/IGNITE-17416
                                ClusterConfigShowCallInput.builder().clusterUrl(session.sessionDetails().nodeUrl()).build()
                        ).body().getValue()
                ));
                ;
            } catch (Exception ignored) {
                // no-op
            }
        });
    }

    @Override
    public void onDisconnect() {

    }

    public Config config() {
        return config.get();
    }
}
