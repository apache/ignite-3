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

import static org.apache.ignite.internal.CompatibilityTestCommon.createDefaultTables;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.compute.PutVaultEntriesJob.NEW_VALUE;
import static org.apache.ignite.internal.compute.PutVaultEntriesJob.OVERWRITTEN_KEY;
import static org.apache.ignite.internal.compute.PutVaultEntriesJob.REMOVED_KEY;
import static org.apache.ignite.internal.compute.PutVaultEntriesJob.TEST_KEY;
import static org.apache.ignite.internal.compute.PutVaultEntriesJob.TEST_VALUE;
import static org.apache.ignite.internal.jobs.DeploymentUtils.runJob;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.compute.PutVaultEntriesJob;
import org.apache.ignite.internal.jobs.DeploymentUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/** Compatibility tests for vault storage. */
@ParameterizedClass
@MethodSource("baseVersions")
@MicronautTest(rebuildContext = true)
public class ItVaultStorageCompatibilityTest extends CompatibilityTestBase {
    @Override
    protected int nodesCount() {
        return 1;
    }

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        DeploymentUtils.deployJobs();

        createDefaultTables(baseIgnite);

        runPutVaultEntriesJob();
    }

    @Test
    void testVaultStorageCompatibility() {
        IgniteImpl ignite = unwrapIgniteImpl(cluster.node(0));
        assertThat(ignite.vault().get(TEST_KEY).value(), is(TEST_VALUE));
        assertThat(ignite.vault().get(OVERWRITTEN_KEY).value(), is(NEW_VALUE));
        assertThat(ignite.vault().get(REMOVED_KEY), nullValue());
    }

    private void runPutVaultEntriesJob() {
        runJob(cluster, PutVaultEntriesJob.class, "");
    }
}
