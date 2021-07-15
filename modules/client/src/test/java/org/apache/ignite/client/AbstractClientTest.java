package org.apache.ignite.client;

import io.netty.channel.ChannelFuture;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.helpers.NOPLogger;

import java.util.Collections;

public abstract class AbstractClientTest {
    protected static final String DEFAULT_TABLE = "default_test_table";

    protected static ChannelFuture serverFuture;

    protected static Ignite server;

    protected static Ignite client;

    @BeforeAll
    public static void beforeAll() throws Exception {
        serverFuture = startServer();
        client = startClient();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        client.close();
        serverFuture.cancel(true);
        serverFuture.await();
    }

    @BeforeEach
    public void beforeEach() {
        for (var t : server.tables().tables())
            server.tables().dropTable(t.tableName());
    }

    protected static Ignite startClient() {
        var builder = IgniteClient.builder().addresses("127.0.0.2:10800");

        return builder.build();
    }

    protected static ChannelFuture startServer() throws InterruptedException {
        var registry = new ConfigurationRegistry(
                Collections.singletonList(ClientConnectorConfiguration.KEY),
                Collections.emptyMap(),
                Collections.singletonList(new TestConfigurationStorage(ConfigurationType.LOCAL))
        );

        server = new FakeIgnite();

        var module = new ClientHandlerModule(server, NOPLogger.NOP_LOGGER);

        module.prepareStart(registry);

        return module.start();
    }
}
