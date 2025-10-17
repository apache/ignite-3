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
import java.util.Map;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.migrationtools.tests.e2e.custom.MySqlFunctions;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.ExampleBasedCacheTest;
import org.jetbrains.annotations.Nullable;

/** MySimpleMapCacheTest. */
@AutoService(ExampleBasedCacheTest.class)
public class MySimpleMapCacheTest extends VeryBasicAbstractCacheTest<String, Integer> {
    private static final String SCHEMA_NAME_CASED = "My_Custom_Schema";

    public MySimpleMapCacheTest() {
        super(String.class, Integer.class, "ID");
    }

    @Override
    public CacheConfiguration<String, Integer> cacheConfiguration() {
        CacheConfiguration<String, Integer> cfg = super.cacheConfiguration();
        cfg.setSqlSchema(SCHEMA_NAME_CASED);
        cfg.setSqlFunctionClasses(MySqlFunctions.class);
        return cfg;
    }

    @Override
    public String getTableName() {
        return "MySimpleMap";
    }

    @Nullable
    @Override
    public String getSchemaName() {
        return SCHEMA_NAME_CASED.toUpperCase();
    }

    @Override
    public Map.Entry<String, Integer> supplyExample(int seed) {
        return Map.entry("MyKey:" + seed, seed);
    }

    @Override
    protected void assertResultSet(ResultSet rs, Integer expectedObj) throws SQLException {
        assertThat(rs.getInt("VAL")).isEqualTo(expectedObj);
    }
}
