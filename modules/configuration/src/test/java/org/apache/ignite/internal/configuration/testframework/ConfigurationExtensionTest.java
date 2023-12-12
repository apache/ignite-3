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

package org.apache.ignite.internal.configuration.testframework;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.configuration.sample.DiscoveryConfiguration;
import org.apache.ignite.internal.configuration.sample.ExtendedDiscoveryConfiguration;
import org.apache.ignite.internal.configuration.sample.ExtendedDiscoveryConfigurationSchema;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test basic scenarios of {@link ConfigurationExtension}.
 */
@ExtendWith(ConfigurationExtension.class)
class ConfigurationExtensionTest extends BaseIgniteAbstractTest {
    /** Injected field. */
    @InjectConfiguration(extensions = ExtendedDiscoveryConfigurationSchema.class)
    private DiscoveryConfiguration fieldCfg;

    @BeforeAll
    static void staticParameterInjection(
            @InjectConfiguration(extensions = ExtendedDiscoveryConfigurationSchema.class) DiscoveryConfiguration paramCfg
    ) {
        assertThat(paramCfg.joinTimeout().update(100), willCompleteSuccessfully());

        assertEquals(100, paramCfg.joinTimeout().value());
    }

    /** Test that contains injected parameter. */
    @Test
    public void injectConfiguration(
            @InjectConfiguration("mock.joinTimeout=100") DiscoveryConfiguration paramCfg
    ) throws Exception {
        assertEquals(5000, fieldCfg.joinTimeout().value());

        assertEquals(100, paramCfg.joinTimeout().value());

        paramCfg.change(d -> d.changeJoinTimeout(200)).get(1, SECONDS);

        assertEquals(200, paramCfg.joinTimeout().value());

        paramCfg.joinTimeout().update(300).get(1, SECONDS);

        assertEquals(300, paramCfg.joinTimeout().value());
    }

    /** Tests that notifications work on injected configuration instance. */
    @Test
    public void notifications() throws Exception {
        List<String> log = new ArrayList<>();

        fieldCfg.listen(ctx -> {
            log.add("update");

            return nullCompletedFuture();
        });

        fieldCfg.joinTimeout().listen(ctx -> {
            log.add("join");

            return nullCompletedFuture();
        });

        fieldCfg.failureDetectionTimeout().listen(ctx -> {
            log.add("failure");

            return nullCompletedFuture();
        });

        fieldCfg.change(change -> change.changeJoinTimeout(1000_000)).get(1, SECONDS);

        assertEquals(List.of("update", "join"), log);

        log.clear();

        fieldCfg.failureDetectionTimeout().update(2000_000).get(1, SECONDS);

        assertEquals(List.of("update", "failure"), log);
    }

    /** Tests that internal configuration extensions work properly on injected configuration instance. */
    @Test
    public void internalConfiguration(
            @InjectConfiguration(extensions = {ExtendedConfigurationSchema.class}) BasicConfiguration cfg
    ) throws Exception {
        assertThat(cfg, is(instanceOf(ExtendedConfiguration.class)));

        assertEquals(1, cfg.visible().value());

        assertEquals(2, ((ExtendedConfiguration) cfg).invisible().value());

        cfg.change(change -> {
            assertThat(change, is(instanceOf(ExtendedChange.class)));

            change.changeVisible(3);

            ((ExtendedChange) change).changeInvisible(4);
        }).get(1, SECONDS);

        assertEquals(3, cfg.visible().value());

        assertEquals(4, ((ExtendedConfiguration) cfg).invisible().value());
    }

    /** Test UUID generation in mocks. */
    @Test
    public void testInjectInternalId(
            @InjectConfiguration(
                    extensions = ExtendedDiscoveryConfigurationSchema.class,
                    name = "test"
            ) DiscoveryConfiguration discoveryConfig
    ) {
        assertNotNull(((ExtendedDiscoveryConfiguration) discoveryConfig).id().value());
    }
}
