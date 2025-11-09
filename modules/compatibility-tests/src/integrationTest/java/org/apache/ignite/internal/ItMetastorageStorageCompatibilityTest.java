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

import static org.apache.ignite.internal.CompatibilityTestCommon.TABLE_NAME_TEST;
import static org.apache.ignite.internal.CompatibilityTestCommon.createDefaultTables;
import static org.apache.ignite.internal.jobs.DeploymentUtils.runJob;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.compute.SendAllMetastorageCommandTypesJob;
import org.apache.ignite.internal.jobs.DeploymentUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/** Compatibility tests for metastorage storage. */
@ParameterizedClass
@MethodSource("baseVersions")
@MicronautTest(rebuildContext = true)
public class ItMetastorageStorageCompatibilityTest extends CompatibilityTestBase {
    @Override
    protected int nodesCount() {
        return 1;
    }

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        DeploymentUtils.deployJobs();

        createDefaultTables(baseIgnite);

        runSendAllMetastorageCommandTypesJob();
    }

    @Test
    void testMetastorageStorageCompatibility() {
        checkMetastorage();
    }

    private void checkMetastorage() {
        // Will fail if metastorage is corrupted.
        sql("SELECT * FROM " + TABLE_NAME_TEST);
    }

    private void runSendAllMetastorageCommandTypesJob() {
        runJob(cluster, SendAllMetastorageCommandTypesJob.class, "");
    }
}
