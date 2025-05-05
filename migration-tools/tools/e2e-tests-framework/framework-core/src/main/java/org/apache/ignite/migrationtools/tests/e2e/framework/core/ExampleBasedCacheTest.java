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
