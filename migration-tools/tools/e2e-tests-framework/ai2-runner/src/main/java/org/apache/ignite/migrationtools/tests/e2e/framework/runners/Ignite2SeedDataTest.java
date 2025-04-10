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

package org.apache.ignite.migrationtools.tests.e2e.framework.runners;

import static org.junit.jupiter.api.Named.named;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.DiscoveryUtils;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.ExampleBasedCacheTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * This is not really a test. It was just easier to build the seed data script like this.
 */
public class Ignite2SeedDataTest {

    private static String CONFIG_URI;
    private static int N_TEST_EXAMPLES = 2_500;

    private static Ignite client = null;

    private static int nCachesPerStint;

    private int currStint = 0;

    @BeforeAll
    static void getConfigurationUri() {
        CONFIG_URI = System.getenv("CONFIG_URI");
        if (CONFIG_URI == null) {
            Assertions.fail("CONFIG_URI environment variable is null. Place provide a valid configuration file");
        }
    }

    @BeforeAll
    static void setNumberOfSamples() {
        var numSamples = System.getenv("N_TEST_SAMPLES");
        if (numSamples != null) {
            N_TEST_EXAMPLES = Integer.parseUnsignedInt(numSamples);
        }
    }

    @BeforeAll
    static void setNumberOfCachesPerStint() {
        nCachesPerStint = Integer.parseInt(System.getProperty("seeddata.nCachesPerStint", "25"));
    }

    private static Stream<Arguments> provideTestArgs() {
        return DiscoveryUtils.discoverClasses().stream()
                .map(tc -> Arguments.of(named(String.format("[%s] - %s", tc.getClass().getSimpleName(), tc.getTableName()), tc)));
    }

    @BeforeEach
    void setupClient() {
        synchronized (Ignite2SeedDataTest.class) {
            if (client == null) {
                Ignition.setClientMode(true);
                client = Ignition.start(CONFIG_URI);

                // Active the cluster if it is innactive.
                // TODO: Control this with a flag.
                ClusterState state = client.cluster().state();
                if (state == ClusterState.INACTIVE) {
                    System.out.println("The cluster was not active. Activating..");
                    client.cluster().state(ClusterState.ACTIVE);
                }
            }
        }
    }

    @AfterEach
    void rotateClient() {
        if (++currStint % nCachesPerStint == 0) {
            tearDownClient();
        }
    }

    @AfterAll
    static void tearDownClient() {
        // TODO: Should optitionally call the cluster shutdown
        synchronized (Ignite2SeedDataTest.class) {
            if (client != null) {
                client.close();
                client = null;
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideTestArgs")
    void seedCache(ExampleBasedCacheTest test) {
        CacheConfiguration cacheCfg = test.cacheConfiguration();

        IgniteCache cache = client.getOrCreateCache(cacheCfg);
        Assertions.assertThat(cache).isNotNull();

        populateCache(cacheCfg.getName(), test::supplyExample);
    }

    private <K, V> void populateCache(String cacheName, Function<Integer, Map.Entry<K, V>> entrySupplier) {
        try (IgniteDataStreamer<K, V> streamer = client.dataStreamer(cacheName)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < N_TEST_EXAMPLES; i++) {
                Map.Entry<K, V> entry = entrySupplier.apply(i);
                streamer.addData(entry.getKey(), entry.getValue());

                if (i > 0 && i % 10_000 == 0) {
                    System.out.println("Done: " + i);
                }
            }
        }
    }
}
