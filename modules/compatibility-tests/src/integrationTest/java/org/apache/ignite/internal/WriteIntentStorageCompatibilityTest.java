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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.compute.DisableWriteIntentSwitchExecutionJob;
import org.apache.ignite.internal.jobs.DeploymentUtils;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

@ParameterizedClass
@MethodSource("baseVersions")
class WriteIntentStorageCompatibilityTest extends CompatibilityTestBase {
    private static final String TEST_TABLE = "TEST_TABLE";

    @Override
    protected int nodesCount() {
        return 1;
    }

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) throws Exception {
        DeploymentUtils.deployJobs();

        disableWriteIntentSwitchExecution();

        baseIgnite.sql().executeScript("CREATE TABLE " + TEST_TABLE + " (ID INT PRIMARY KEY, VAL VARCHAR)");
        KeyValueView<Integer, String> view = baseIgnite.tables().table(TEST_TABLE).keyValueView(Integer.class, String.class);

        baseIgnite.transactions().runInTransaction(tx -> {
            view.put(tx, 1, "one");
            view.put(tx, 2, "two");
            view.put(tx, 3, "three");
        });
    }

    private void disableWriteIntentSwitchExecution() {
        DeploymentUtils.runJob(cluster, DisableWriteIntentSwitchExecutionJob.class, null);
    }

    @Test
    void writeIntentsOnNewVersionAreResolved() {
        KeyValueView<Integer, String> view = node(0).tables().table(TEST_TABLE).keyValueView(Integer.class, String.class);

        assertThat(view.get(null, 1), is("one"));
        assertThat(view.get(null, 2), is("two"));
        assertThat(view.get(null, 3), is("three"));
    }
}
