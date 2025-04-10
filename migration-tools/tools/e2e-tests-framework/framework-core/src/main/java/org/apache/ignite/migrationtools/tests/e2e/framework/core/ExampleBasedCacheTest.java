/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tests.e2e.framework.core;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.table.Table;

/**
 * Interface for example based tests.
 *
 * @param <K> Type of the key.
 * @param <V> Type of the value.
 */
public interface ExampleBasedCacheTest<K, V> {

    String getTableName();

    CacheConfiguration<K, V> cacheConfiguration();

    // Generator function
    Map.Entry<K, V> supplyExample(int seed);

    // Ignite 2 Interface test
    void testIgnite2(IgniteCache<K, V> cache, int numGeneratedExamples);

    void testIgnite3(Table ignite3Table, int numGeneratedExamples);

    /**
     * This method exposes references to JDBC Tests.
     * TODO: This still does not feel a good way of exporting the results.
     *  Probably, it would be better to have an annotation directly on the method, like @TestTemplate.
     *  The main issue is that we don't want to load the whole class because it depends on both ignite 2 and ignite 3 classes.
     */
    default Map<String, SqlTest> jdbcTests() {
        return Collections.emptyMap();
    }

}
