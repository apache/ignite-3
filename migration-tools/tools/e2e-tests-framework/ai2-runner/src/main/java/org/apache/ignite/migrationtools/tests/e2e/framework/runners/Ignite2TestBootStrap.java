/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tests.e2e.framework.runners;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Named.named;

import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.DiscoveryUtils;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.ExampleBasedCacheTest;
import org.apache.ignite.migrationtools.tests.e2e.impl.VeryBasicAbstractCacheTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Ignite2TestBootStrap. */
public class Ignite2TestBootStrap {

    private static String CONFIG_URI;
    private static int N_TEST_EXAMPLES = 2_500;

    private final Supplier<Ignite> clientSupplier;

    private Ignite client = null;

    public Ignite2TestBootStrap() {
        this(Ignite2TestBootStrap::createClient);
    }

    public Ignite2TestBootStrap(Supplier<Ignite> clientSupplier) {
        this.clientSupplier = clientSupplier;
    }

    private static Ignite createClient() {
        Ignition.setClientMode(true);
        return Ignition.start(CONFIG_URI);
    }

    @BeforeAll
    static void getConfigurationUri() {
        CONFIG_URI = System.getenv("CONFIG_URI");
        if (CONFIG_URI == null) {
            fail("CONFIG_URI environment variable is null. Place provide a valid configuration file");
        }
    }

    @BeforeAll
    static void setNumberOfSamples() {
        var numSamples = System.getenv("N_TEST_SAMPLES");
        if (numSamples != null) {
            N_TEST_EXAMPLES = Integer.parseUnsignedInt(numSamples);
        }
    }

    private static Stream<Arguments> provideTestArgs() {
        return DiscoveryUtils.discoverClasses().stream()
                .filter(tc -> tc instanceof VeryBasicAbstractCacheTest)
                .map(tc -> Arguments.of(named(String.format("[%s] - %s", tc.getClass().getSimpleName(), tc.getTableName()), tc)));
    }

    @BeforeEach
    void setupClient() {
        this.client = this.clientSupplier.get();
    }

    @AfterEach
    void tearDownClient() {
        this.client.close();
        this.client = null;
    }

    @DisplayName("Ignite 2 Java API Tests")
    @ParameterizedTest
    @MethodSource("provideTestArgs")
    void runTest(ExampleBasedCacheTest test) {
        IgniteCache cache = this.client.cache(test.getTableName());
        assertThat(cache).isNotNull();

        test.testIgnite2(cache, N_TEST_EXAMPLES);
    }
}
