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
            // TODO: IGNITE-25440 Implement a simple test checking if the column have content.
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
