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
            exclude = ""
                    + "org.apache.ignite.Ignite#clusterNodes();" // deprecated
                    + "org.apache.ignite.Ignite#clusterNodesAsync();" // deprecated
                    + "org.apache.ignite.catalog.IgniteCatalog#dropTable(java.lang.String);" // method abstract now default
                    + "org.apache.ignite.catalog.IgniteCatalog#dropTableAsync(java.lang.String);" // method abstract now default
                    + "org.apache.ignite.catalog.IgniteCatalog#tableDefinition(java.lang.String);" // method abstract now default
                    + "org.apache.ignite.catalog.IgniteCatalog#tableDefinitionAsync(java.lang.String);" // method abstract now default
                    + "org.apache.ignite.compute.ColocatedJobTarget;" // method return type changed
                    + "org.apache.ignite.compute.TableJobTarget;" // method return type changed
                    + "org.apache.ignite.lang.ColumnNotFoundException;" // deprecated
                    + "org.apache.ignite.lang.IndexAlreadyExistsException;" // deprecated
                    + "org.apache.ignite.lang.IndexNotFoundException;" // deprecated
                    + "org.apache.ignite.lang.TableAlreadyExistsException;" // deprecated
                    + "org.apache.ignite.lang.TableNotFoundException;" // constructor removed
                    + "org.apache.ignite.lang.util.IgniteNameUtils;" // methods removed, less accessible
                    + "org.apache.ignite.sql.IgniteSql;" // method abstract now default
                    + "org.apache.ignite.table.DataStreamerTarget;" // method abstract now default
                    + "org.apache.ignite.table.IgniteTables;" // method abstract now default
                    + "org.apache.ignite.table.QualifiedName;" // now final, serializable
                    + "org.apache.ignite.table.Table;" // method abstract now default
    )
    void testApiModule(CompatibilityOutput output) {}

}
