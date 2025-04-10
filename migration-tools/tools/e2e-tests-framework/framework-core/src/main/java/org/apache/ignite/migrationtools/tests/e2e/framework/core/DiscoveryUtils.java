/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tests.e2e.framework.core;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** DiscoveryUtils. */
public class DiscoveryUtils {

    private DiscoveryUtils() {
        // Intentionally Left Blank
    }

    /**
     * Discovers all the test class implementations.
     *
     * @return The discovered test class implementations.
     */
    public static List<ExampleBasedCacheTest> discoverClasses() {
        // Load Service Providers
        var clsFromSrvcProvider = ServiceLoader.load(ExampleBasedCacheTestProvider.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .flatMap(p -> p.provideTestClasses().stream());

        var directClasses = ServiceLoader.load(ExampleBasedCacheTest.class)
                .stream()
                .map(ServiceLoader.Provider::get);

        return Stream.concat(directClasses, clsFromSrvcProvider).collect(Collectors.toList());
    }

}
