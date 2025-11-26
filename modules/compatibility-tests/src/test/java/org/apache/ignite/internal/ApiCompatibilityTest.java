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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.compatibility.api.CompatibilityChecker;
import org.apache.ignite.internal.properties.IgniteProperties;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ApiCompatibilityTest {
    // Change to true to test compatibility between consecutive old versions
    private static final boolean INCLUDE_HISTORIC_DATA = false;

    // Map from old version to the list of excludes for the consecutive version.
    // Read this as "what is broken after this version"
    private static final Map<String, List<String>> EXCLUDES_PER_VERSION = Map.of(
            "3.0.0", List.of(
                    "org.apache.ignite.Ignite#clusterNodes()", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.Ignite#clusterNodesAsync()", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.catalog.IgniteCatalog#dropTable(java.lang.String)", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.catalog.IgniteCatalog#dropTableAsync(java.lang.String)", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.catalog.IgniteCatalog#tableDefinition(java.lang.String)", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.catalog.IgniteCatalog#tableDefinitionAsync(java.lang.String)", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.compute.ColocatedJobTarget#tableName()", // METHOD_RETURN_TYPE_CHANGED
                    "org.apache.ignite.compute.TableJobTarget#tableName()", // METHOD_RETURN_TYPE_CHANGED
                    "org.apache.ignite.lang.ColumnNotFoundException#ColumnNotFoundException(*, *, *)", // CONSTRUCTOR_REMOVED
                    "org.apache.ignite.lang.IndexAlreadyExistsException#IndexAlreadyExistsException(*, *)", // CONSTRUCTOR_REMOVED
                    "org.apache.ignite.lang.IndexNotFoundException#IndexNotFoundException(*, *)", // CONSTRUCTOR_REMOVED
                    "org.apache.ignite.lang.TableAlreadyExistsException#TableAlreadyExistsException(*, *)", // CONSTRUCTOR_REMOVED
                    "org.apache.ignite.lang.TableNotFoundException#TableNotFoundException(*, *)", // CONSTRUCTOR_REMOVED
                    "org.apache.ignite.lang.util.IgniteNameUtils#canonicalOrSimpleName(java.lang.String)", // METHOD_REMOVED
                    "org.apache.ignite.lang.util.IgniteNameUtils#parseSimpleName(java.lang.String)", // METHOD_REMOVED
                    "org.apache.ignite.lang.util.IgniteNameUtils#identifierExtend(int)", // METHOD_LESS_ACCESSIBLE
                    "org.apache.ignite.lang.util.IgniteNameUtils#identifierStart(int)", // METHOD_LESS_ACCESSIBLE
                    "org.apache.ignite.lang.util.IgniteNameUtils#quote(java.lang.String)", // METHOD_LESS_ACCESSIBLE
                    "org.apache.ignite.sql.IgniteSql#executeBatch(*, *, *)", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.sql.IgniteSql#executeBatch(*, *)", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.sql.IgniteSql#executeBatchAsync(*, *, *)", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.sql.IgniteSql#executeBatchAsync(*, *)", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.table.DataStreamerTarget#streamData(*, *, *, *, *, *, *)", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.table.IgniteTables#table(java.lang.String)", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.table.IgniteTables#tableAsync(java.lang.String)", // METHOD_ABSTRACT_NOW_DEFAULT
                    "org.apache.ignite.table.QualifiedName", // CLASS_NOW_FINAL
                    "org.apache.ignite.table.Table#name()" // METHOD_ABSTRACT_NOW_DEFAULT
            ),
            "3.1.0", List.of(
                    // Erroneous error code remove between versions
                    "org.apache.ignite.lang.ErrorGroups$DisasterRecovery#RESTART_WITH_CLEAN_UP_ERR"
            )
    );

    @ParameterizedTest(name = "Old version: {0}, new version: {1}")
    @MethodSource("versions")
    void testApiModule(String oldVersion, String newVersion) {
        CompatibilityChecker.builder()
                .module("ignite-api")
                .oldVersion(oldVersion)
                .newVersion(newVersion)
                .exclude(EXCLUDES_PER_VERSION.get(oldVersion))
                .check();
    }

    private static List<Arguments> versions() {
        List<Arguments> result = new ArrayList<>();

        List<String> versions = new ArrayList<>(IgniteVersions.INSTANCE.versions().keySet());

        if (INCLUDE_HISTORIC_DATA) {
            for (int i = 1; i < versions.size(); i++) {
                String oldVersion = versions.get(i - 1);
                String newVersion = versions.get(i);
                result.add(Arguments.of(oldVersion, newVersion));
            }
        }

        // Current against latest released
        String oldVersion = versions.get(versions.size() - 1);
        String newVersion = IgniteProperties.get(IgniteProperties.VERSION);
        result.add(Arguments.of(oldVersion, newVersion));

        return result;
    }
}
