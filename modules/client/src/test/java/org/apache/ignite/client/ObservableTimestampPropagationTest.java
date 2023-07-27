package org.apache.ignite.client;

import io.netty.util.ResourceLeakDetector;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ObservableTimestampPropagationTest extends AbstractClientTest {
    private static TestServer testServer2;

    private static Ignite server2;

    private static IgniteClient client2;

    /**
     * Before all.
     */
    @BeforeAll
    public static void startServer2() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        TestHybridClock clock = new TestHybridClock(() -> 1000L);

        server2 = new FakeIgnite("server-2");
        testServer2 = new TestServer(0, server2, null, null, "server-2", clusterId, null, serverPort + 1, clock);

        var clientBuilder = IgniteClient.builder()
                .addresses("127.0.0.1:" + serverPort, "127.0.0.1:" + testServer2.port())
                .heartbeatInterval(200);

        client2 = clientBuilder.build();
    }

    /**
     * After all.
     */
    @AfterAll
    public static void stopServer2() throws Exception {
        IgniteUtils.closeAll(client2, testServer2, server2);
    }

    @Test
    public void testClientPropagatesLatestKnownHybridTimestamp() {
        // TODO
    }
}
