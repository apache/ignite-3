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

package org.apache.ignite.migrationtools.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.migrationtools.sql.SqlDdlGenerator;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** CacheDdlCreationFromWorkDirTest. */
@DisabledIfSystemProperty(
        named = "tests.containers.support",
        matches = "false",
        disabledReason = "Lack of support in TeamCity for testcontainers")
@ExtendWith(FullSampleCluster.class)
public class CacheDdlCreationFromWorkDirTest {
    private static MigrationKernalContext NODE_CTX;

    @BeforeAll
    static void initNodeCtx() throws IOException, IgniteCheckedException {
        NODE_CTX = BasePersistentTestContext.recreateNodeContexes().get(0);
        NODE_CTX.start();
    }

    @AfterAll
    static void closeNodeCtx() throws IgniteCheckedException {
        if (NODE_CTX != null) {
            NODE_CTX.stop();
        }
    }

    private static String generateSqlDdlForCache(String cacheName) {
        var cacheDescriptor = NODE_CTX.cache().cacheDescriptor(cacheName);
        var cacheCfg = cacheDescriptor.cacheConfiguration();

        SqlDdlGenerator gen = new SqlDdlGenerator();
        var tableDef = gen.generateTableDefinition(cacheCfg);
        return SqlDdlGenerator.createDdlQuery(tableDef);
    }

    @Test
    void testCreateDdlForMySimpleCache() {
        var sqlStr = generateSqlDdlForCache("MySimpleMap");
        assertThat(sqlStr)
                .startsWith("CREATE TABLE MY_CUSTOM_SCHEMA.\"MySimpleMap\"")
                .contains("PRIMARY KEY (ID)")
                .contains("ID VARCHAR")
                .contains("VAL INT");
    }

    @Test
    void testCreateDdlForMyPersonPojoCache() {
        var sqlStr = generateSqlDdlForCache("MyPersonPojoCache");
        assertThat(sqlStr)
                .startsWith("CREATE TABLE PUBLIC.\"MyPersonPojoCache\"")
                .contains("KEY INT")
                .contains("PRIMARY KEY (KEY)")
                .contains("ID BIGINT")
                .contains("ORGID BIGINT")
                .contains("FIRSTNAME VARCHAR")
                .contains("LASTNAME VARCHAR")
                .contains("RESUME VARCHAR")
                .contains("SALARY DOUBLE");
    }

    @Test
    void testCreateDdlForMyOrgPojoCache() {
        var sqlStr = generateSqlDdlForCache("MyOrganizations");
        assertThat(sqlStr)
                .startsWith("CREATE TABLE PUBLIC.\"MyOrganizations\"")
                .contains("KEY BIGINT")
                .contains("PRIMARY KEY (KEY)")
                .contains("ID BIGINT")
                .contains("NAME VARCHAR");
    }

    @ParameterizedTest
    @ValueSource(strings = {"MyIntArrCache", "MyListArrCache"})
    void testCollectionsCaches(String cacheName) {
        var sqlStr = generateSqlDdlForCache(cacheName);
        assertThat(sqlStr)
                .startsWith("CREATE TABLE PUBLIC.\"" + cacheName + "\"")
                .contains("PRIMARY KEY (ID)")
                .contains("ID INT")
                .contains("VAL VARBINARY(1024)");
    }

    @ParameterizedTest
    @ValueSource(strings = {"MyBinaryPersonPojoCache", "MyBinaryOrganizationCache", "MyBinaryTestCache"})
    void testCreateDdlForBinaryCache(String cacheName) {
        var sqlStr = generateSqlDdlForCache(cacheName);
        assertThat(sqlStr)
                .startsWith("CREATE TABLE PUBLIC.\"" + cacheName + "\"")
                .contains("PRIMARY KEY (ID)")
                .contains("ID VARBINARY(1024)")
                .contains("VAL VARBINARY(1024)");
    }
}
