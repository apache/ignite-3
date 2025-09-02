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

import org.apache.ignite.internal.compatibility.api.ApiCompatibilityTest;
import org.apache.ignite.internal.compatibility.api.CompatibilityOutput;

class ItApiCompatibilityTest {

    // TODO resolve or explain exclusions https://issues.apache.org/jira/browse/IGNITE-26365
    @ApiCompatibilityTest(
            newVersion = "3.1.0-SNAPSHOT",
            oldVersions = "3.0.0",
            exclude = "org.apache.ignite.Ignite#clusterNodes();"
                    + "org.apache.ignite.Ignite#clusterNodesAsync();"
                    + "org.apache.ignite.catalog.IgniteCatalog#dropTable(java.lang.String);"
                    + "org.apache.ignite.catalog.IgniteCatalog#dropTableAsync(java.lang.String);"
                    + "org.apache.ignite.catalog.IgniteCatalog#tableDefinition(java.lang.String);"
                    + "org.apache.ignite.catalog.IgniteCatalog#tableDefinitionAsync(java.lang.String);"
                    + "org.apache.ignite.compute.ColocatedJobTarget;"
                    + "org.apache.ignite.compute.TableJobTarget;"
                    + "org.apache.ignite.lang.ColumnNotFoundException;"
                    + "org.apache.ignite.lang.IndexAlreadyExistsException;"
                    + "org.apache.ignite.lang.IndexNotFoundException;"
                    + "org.apache.ignite.lang.TableAlreadyExistsException;"
                    + "org.apache.ignite.lang.TableNotFoundException;"
                    + "org.apache.ignite.lang.util.IgniteNameUtils;"
                    + "org.apache.ignite.sql.IgniteSql;"
                    + "org.apache.ignite.table.DataStreamerTarget;"
                    + "org.apache.ignite.table.IgniteTables;"
                    + "org.apache.ignite.table.QualifiedName;"
                    + "org.apache.ignite.table.Table;"
    )
    void testApiModule(CompatibilityOutput output) {}

}
