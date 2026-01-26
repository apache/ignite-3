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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.auto.service.AutoService;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.ExampleBasedCacheTest;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Assumptions;

/** IntArrayCachesTest. */
public class IntArrayCachesTest {

    private static final String GROUP_NAME = "CollectionTypeCaches";

    private static final String KEY_COLUMN_NAME = "ID";

    private static final String VAL_COLUMN_NAME = "VAL";

    /** PrimitiveTest. */
    @AutoService(ExampleBasedCacheTest.class)
    public static class PrimitiveTest extends VeryBasicAbstractCacheTest<Integer, int[]> {

        public PrimitiveTest() {
            super(Integer.class, int[].class, KEY_COLUMN_NAME);
        }

        @Override
        public CacheConfiguration<Integer, int[]> cacheConfiguration() {
            return super.cacheConfiguration().setGroupName(GROUP_NAME);
        }

        @Override
        public String getTableName() {
            return "MyIntArrCache";
        }

        @Override
        public Map.Entry<Integer, int[]> supplyExample(int seed) {
            int n1 = seed;
            int n2 = seed * 100;
            int sum = n1 + n2;
            int[] n = new int[] {sum, n1, n2};
            return Map.entry(seed, n);
        }

        @Override
        public void testIgnite3(Table ignite3Table, int numGeneratedExamples) {
            // TODO: IGNITE-27618 Rollout native support.
            Assumptions.abort("There is currently no canonical way of mapping int[] cache to ignite 3");
        }

        @Override
        protected void assertResultSet(ResultSet rs, int[] expectedObj) throws SQLException {
            // TODO: IGNITE-27618
            Assumptions.abort("Currently the JDBC driver does not support adapting binary to concrete types");
            int[] actual = rs.getObject(VAL_COLUMN_NAME, int[].class);
            assertThat(actual).isEqualTo(expectedObj);
        }
    }

    /** ListTest. */
    @AutoService(ExampleBasedCacheTest.class)
    public static class ListTest extends VeryBasicAbstractCacheTest<Integer, List> {

        public ListTest() {
            super(Integer.class, List.class, KEY_COLUMN_NAME);
        }

        @Override
        public CacheConfiguration<Integer, List> cacheConfiguration() {
            return super.cacheConfiguration().setGroupName(GROUP_NAME);
        }

        @Override
        public String getTableName() {
            return "MyListArrCache";
        }

        @Override
        public Map.Entry<Integer, List> supplyExample(int seed) {
            int n1 = seed;
            int n2 = seed * 100;
            int sum = n1 + n2;
            List<Integer> n = List.of(sum, n1, n2);
            return Map.entry(seed, n);
        }

        @Override
        public void testIgnite3(Table ignite3Table, int numGeneratedExamples) {
            // TODO: IGNITE-27618 Rollout native support.
            Assumptions.abort("There is currently no canonical way of mapping List caches to ignite 3");
        }

        @Override
        protected void assertResultSet(ResultSet rs, List expectedObj) throws SQLException {
            // TODO: IGNITE-27618
            Assumptions.abort("Currently the JDBC driver does not support adapting binary to concrete types");
            List<Integer> actual = (List<Integer>) rs.getObject(VAL_COLUMN_NAME);
            assertThat(actual).isEqualTo(expectedObj);
        }
    }

}
