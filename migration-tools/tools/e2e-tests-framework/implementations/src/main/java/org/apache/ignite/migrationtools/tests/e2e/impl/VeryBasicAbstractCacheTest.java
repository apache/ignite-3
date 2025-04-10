/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tests.e2e.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.fail;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.ExampleBasedCacheTest;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.SqlTest;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.SqlTestUtils;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.mapper.Mapper;

/** VeryBasicAbstractCacheTest. */
public abstract class VeryBasicAbstractCacheTest<K, V> implements ExampleBasedCacheTest<K, V> {
    private static final String MAPPER_ERROR_MSG = "Test still not defined for binary caches";

    private final Class<K> keyClass;

    private final Class<V> valueClass;

    private final String keyColumnName;

    public VeryBasicAbstractCacheTest(Class<K> keyClass, Class<V> valueClass) {
        this(keyClass, valueClass, "ID");
    }

    /** Constructor. */
    public VeryBasicAbstractCacheTest(Class<K> keyClass, Class<V> valueClass, String keyColumnName) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.keyColumnName = keyColumnName;
    }

    /** Creates the cache configuration for this test. */
    public static <K, V> CacheConfiguration<K, V> createCacheConfiguration(String name, Class<K> keyType, Class<V> valType) {
        CacheConfiguration<K, V> cacheCfg = new CacheConfiguration<>(name);

        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setIndexedTypes(keyType, valType);
        return cacheCfg;
    }

    @Override
    public CacheConfiguration<K, V> cacheConfiguration() {
        return createCacheConfiguration(this.getTableName(), keyClass, valueClass);
    }

    @SuppressFBWarnings("DMI_RANDOM_USED_ONLY_ONCE")
    @Override
    public void testIgnite2(IgniteCache<K, V> cache, int numGeneratedExamples) {
        // Must have nGenerated examples in the cache
        assertThat(cache.sizeLong()).isEqualTo(numGeneratedExamples);

        // Check if a random example is in the cache.
        Random r = new Random();
        int expectedSeed = r.nextInt(numGeneratedExamples);
        Map.Entry<K, V> expected = supplyExample(expectedSeed);
        V actualVal = cache.get(expected.getKey());

        assertValueFromIgnite2(actualVal, expected.getValue());
    }

    @SuppressFBWarnings("DMI_RANDOM_USED_ONLY_ONCE")
    @Override
    public void testIgnite3(Table ignite3Table, int numGeneratedExamples) {
        var keyMapper = keyMapper();
        var valMapper = valMapper();
        KeyValueView<K, V> kvView = ignite3Table.keyValueView(keyMapper, valMapper);

        Random r = new Random();
        int expectedSeed = r.nextInt(numGeneratedExamples);
        Map.Entry<K, V> expected = supplyExample(expectedSeed);

        V actualVal = kvView.get(null, expected.getKey());
        assertValueFromIgnite3(actualVal, expected.getValue());
    }

    @Override
    public Map<String, SqlTest> jdbcTests() {
        return Map.of(
                "Count Test", (conn, numExamples) -> SqlTestUtils.sqlCountRecordsTest(conn, getIgnite3SqlTableName(), numExamples),
                "Element Iterator Test", (conn, numExamples) ->
                        SqlTestUtils.sqlRandomElementTest(
                                conn,
                                getIgnite3SqlTableName(),
                                keyColumnName,
                                numExamples,
                                this::supplyExample,
                                (expectedObj, resultSet) -> {
                                    try {
                                        assertResultSet(resultSet, expectedObj);
                                    } catch (SQLException e) {
                                        fail(e);
                                    }
                                }
                        )
        );
    }

    // TODO: Check if this is the actual AI2 table name.
    private String getSqlTableName() {
        // Get the tableName for the SQL Query. Why uppercase??
        String tableName = (this.valueClass.isArray()) ? '"' + this.valueClass.getName() + '"' : this.valueClass.getSimpleName();
        return "\"" + this.getTableName() + "\"." + tableName.toUpperCase();
    }

    // TODO: Check if this is the actual AI3 table name.
    private String getIgnite3SqlTableName() {
        return "\"" + this.getTableName() + "\"";
    }

    protected void assertValueFromIgnite2(V actualVal, V expected) {
        assertThat(actualVal).usingRecursiveComparison().isEqualTo(expected);
    }

    protected void assertValueFromIgnite3(V actualVal, V expected) {
        assertThat(actualVal).usingRecursiveComparison().isEqualTo(expected);
    }

    protected void assertResultSet(ResultSet rs, V expectedObj) throws SQLException {
        // Intentionally left blank. Please override in the implementation.
    }

    protected Mapper<K> keyMapper() {
        // TODO: Define a complementary test for Binary Caches
        assumeThat(keyClass).as(MAPPER_ERROR_MSG).isNotNull();
        return Mapper.of(keyClass);
    }

    protected Mapper<V> valMapper() {
        // TODO: Define a complementary test for Binary Caches
        assumeThat(valueClass).as(MAPPER_ERROR_MSG).isNotNull();
        return Mapper.of(valueClass);
    }
}
