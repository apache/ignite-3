/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tests.e2e.impl;

import com.google.auto.service.AutoService;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.ExampleBasedCacheTest;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.SqlTest;

/** MyBinaryKeyValueCacheTest. */
public class MyBinaryKeyValueCacheTest {

    abstract static class AbstractBinaryCache extends VeryBasicAbstractCacheTest<Object, Object> {

        public AbstractBinaryCache() {
            super(null, null);
        }

        @Override
        public CacheConfiguration<Object, Object> cacheConfiguration() {
            CacheConfiguration cacheCfg = new CacheConfiguration<>(this.getTableName());
            cacheCfg.setGroupName("BinaryCaches");
            cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg.setBackups(1);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            return cacheCfg;
        }

        @Override
        public Map<String, SqlTest> jdbcTests() {
            // TODO: GG-38110 Check if this can be implemented
            return Collections.emptyMap();
        }
    }

    /** MyBinaryPersonPojoCache. */
    @AutoService(ExampleBasedCacheTest.class)
    public static class MyBinaryPersonPojoCache extends AbstractBinaryCache {

        @Override
        public String getTableName() {
            return "MyBinaryPersonPojoCache";
        }

        @Override
        public Map.Entry<Object, Object> supplyExample(int seed) {
            PersonCacheTest delegate = new PersonCacheTest();
            var example = delegate.supplyExample(seed);
            return Map.entry((Object) example.getKey(), (Object) example.getValue());
        }

    }

    /** MyBinaryOrganizationCache. */
    @AutoService(ExampleBasedCacheTest.class)
    public static class MyBinaryOrganizationCache extends AbstractBinaryCache {

        @Override
        public String getTableName() {
            return "MyBinaryOrganizationCache";
        }

        @Override
        public Map.Entry<Object, Object> supplyExample(int seed) {
            MyOrganizationsCacheTest delegate = new MyOrganizationsCacheTest();
            var example = delegate.supplyExample(seed);
            return Map.entry((Object) example.getKey(), (Object) example.getValue());
        }

    }

    /** MyBinaryTestCache. */
    @AutoService(ExampleBasedCacheTest.class)
    public static class MyBinaryTestCache extends AbstractBinaryCache {

        @Override
        public String getTableName() {
            return "MyBinaryTestCache";
        }

        @Override
        public Map.Entry<Object, Object> supplyExample(int seed) {
            String key = "MyKey:" + seed;
            String myVal = "MyValue" + seed;
            return Map.entry(key.getBytes(StandardCharsets.UTF_8), myVal.getBytes(StandardCharsets.UTF_8));
        }

    }

}
