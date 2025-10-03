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

package org.apache.ignite;

import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_BACKGROUND_RECONNECT_INTERVAL;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_CONNECT_TIMEOUT;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_HEARTBEAT_INTERVAL;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_HEARTBEAT_TIMEOUT;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_OPERATION_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.apache.ignite.client.BasicAuthenticator;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.RetryReadPolicy;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link IgniteClientAutoConfiguration}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class IgniteClientAutoConfigurationTest extends BaseIgniteAbstractTest {

    @WorkDirectory
    private Path workDir;

    private Cluster cluster;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(testInfo, workDir).build();

        this.cluster = new Cluster(clusterConfiguration);
        this.cluster.startAndInit(1);
    }

    @AfterEach
    void tearDown() {
        cluster.shutdown();
    }

    @Test
    void testDefaultValues() {
        ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withPropertyValues("ignite.client.addresses=127.0.0.1:10800")
                .withConfiguration(AutoConfigurations.of(IgniteClientAutoConfiguration.class));

        contextRunner.run(context -> {
            IgniteClient client = context.getBean(IgniteClient.class);

            assertEquals(DFLT_CONNECT_TIMEOUT, client.configuration().connectTimeout());
            assertEquals(DFLT_HEARTBEAT_TIMEOUT, client.configuration().heartbeatTimeout());
            assertEquals(DFLT_HEARTBEAT_INTERVAL, client.configuration().heartbeatInterval());
            assertEquals(DFLT_BACKGROUND_RECONNECT_INTERVAL, client.configuration().backgroundReconnectInterval());
            assertEquals(DFLT_OPERATION_TIMEOUT, client.configuration().operationTimeout());
        });
    }

    @Test
    void testSetValues() {
        boolean metricsEnabled = true;
        long connectTimeout = 1111;
        long operationTimeout = 2222;
        long backgroundReconnectInterval = 3333;
        long heartbeatInterval = 4444;
        long heartbeatTimeout = 5555;

        ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withPropertyValues(
                        "ignite.client.addresses=127.0.0.1:10800",
                        "ignite.client.metricsEnabled=" + metricsEnabled,
                        "ignite.client.connectTimeout=" + connectTimeout,
                        "ignite.client.operationTimeout=" + operationTimeout,
                        "ignite.client.backgroundReconnectInterval=" + backgroundReconnectInterval,
                        "ignite.client.heartbeatInterval=" + heartbeatInterval,
                        "ignite.client.heartbeatTimeout=" + heartbeatTimeout)
                .withConfiguration(AutoConfigurations.of(IgniteClientAutoConfiguration.class));

        contextRunner.run(context -> {
            IgniteClient client = context.getBean(IgniteClient.class);

            assertEquals(metricsEnabled, client.configuration().metricsEnabled());
            assertEquals(connectTimeout, client.configuration().connectTimeout());
            assertEquals(heartbeatTimeout, client.configuration().heartbeatTimeout());
            assertEquals(heartbeatInterval, client.configuration().heartbeatInterval());
            assertEquals(backgroundReconnectInterval, client.configuration().backgroundReconnectInterval());
            assertEquals(operationTimeout, client.configuration().operationTimeout());
        });
    }

    @Test
    void testCustomizer() {
        ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(IgniteClientAutoConfiguration.class))
                .withPropertyValues("ignite.client.addresses=127.0.0.1:10800")
                .withBean(IgniteClientPropertiesCustomizer.class, () -> (c) -> c.setRetryPolicy(new RetryReadPolicy()));

        contextRunner.run((ctx) -> {
            IgniteClient client = ctx.getBean(IgniteClient.class);
            assertEquals(RetryReadPolicy.class, client.configuration().retryPolicy().getClass());
        });
    }

    @Test
    void testCustomizerOverrides() {
        long connectTimeout = 8888L;

        ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(IgniteClientAutoConfiguration.class))
                .withPropertyValues("ignite.client.addresses=127.0.0.1:10800", "ignite.client.connectTimeoutMillis=99999")
                .withBean(IgniteClientPropertiesCustomizer.class, () -> (c) -> c.setConnectTimeout(connectTimeout));

        contextRunner.run((ctx) -> {
            IgniteClient client = ctx.getBean(IgniteClient.class);
            assertEquals(connectTimeout, client.configuration().connectTimeout());
        });
    }

    @Test
    void testSetAuthenticatorViaCustomizer() {
        String username = "user";
        String password = "password";

        // check that authenticator is not set by default
        ApplicationContextRunner contextRunnerDefault = new ApplicationContextRunner()
                .withPropertyValues("ignite.client.addresses=127.0.0.1:10800")
                .withConfiguration(AutoConfigurations.of(IgniteClientAutoConfiguration.class));

        contextRunnerDefault.run((ctx) -> {
            IgniteClient client = ctx.getBean(IgniteClient.class);
            assertNull(client.configuration().authenticator());
        });

        // check that authenticator can be set via customizer
        ApplicationContextRunner contextRunnerCustomizer = new ApplicationContextRunner()
                .withPropertyValues("ignite.client.addresses=127.0.0.1:10800")
                .withConfiguration(AutoConfigurations.of(IgniteClientAutoConfiguration.class))
                .withBean(IgniteClientPropertiesCustomizer.class, () -> (c) ->
                        c.setAuthenticator(BasicAuthenticator.builder().username(username).password(password).build())
                );

        contextRunnerCustomizer.run((ctx) -> {
            IgniteClient client = ctx.getBean(IgniteClient.class);
            assertNotNull(client.configuration().authenticator());
        });
    }

    @Test
    void testSetAuthenticatorViaProperties() {
        String username = "user";
        String password = "password";

        // check that authenticator is not set by default
        ApplicationContextRunner contextRunnerDefault = new ApplicationContextRunner()
                .withPropertyValues("ignite.client.addresses=127.0.0.1:10800")
                .withConfiguration(AutoConfigurations.of(IgniteClientAutoConfiguration.class));

        contextRunnerDefault.run((ctx) -> {
            IgniteClient client = ctx.getBean(IgniteClient.class);
            assertNull(client.configuration().authenticator());
        });

        // check that authenticator can be set via properties
        ApplicationContextRunner contextRunnerCustomizer = new ApplicationContextRunner()
                .withPropertyValues(
                        "ignite.client.addresses=127.0.0.1:10800",
                        "ignite.client.auth.basic.username=" + username,
                        "ignite.client.auth.basic.password=" + password)
                .withConfiguration(AutoConfigurations.of(IgniteClientAutoConfiguration.class));

        contextRunnerCustomizer.run((ctx) -> {
            IgniteClient client = ctx.getBean(IgniteClient.class);
            assertNotNull(client.configuration().authenticator());
        });
    }

    @Test
    void testAddressesNotSet() {
        ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(IgniteClientAutoConfiguration.class));

        Exception ex = assertThrows(IllegalStateException.class, () ->
                contextRunner.run(context -> {
                    context.getBean(IgniteClient.class);
                })
        );
        assertCause(IgniteException.class, "Empty addresses", ex);
    }

    private static void assertCause(Class<?> expectedType, String expectedMessage, Exception actual) {
        Throwable ex = actual;
        while (ex != null) {
            if (expectedType.isInstance(ex)) {
                if (ex.getMessage().contains(expectedMessage)) {
                    return;
                } else {
                    throw new AssertionError("Exception '" + expectedType + "' found, but message is different");
                }
            }
            ex = ex.getCause();
        }

        throw new AssertionError("Exception '" + expectedType + "' not found");
    }
}
