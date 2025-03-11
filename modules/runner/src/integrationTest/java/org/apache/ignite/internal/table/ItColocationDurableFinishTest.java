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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;

import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.testframework.WithSystemProperty;

/**
 * Test resending the finish request from the coordinator when the previous attempts failed for any reason.
 */
// TODO Remove https://issues.apache.org/jira/browse/IGNITE-22522
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
public class ItColocationDurableFinishTest extends ItDurableFinishTest {
    @Override
    void testChangedPrimaryOnFinish() throws Exception {
        // No-op. It's required to test only primary change on cleanup within colocation minimalistic tests set.
    }

    @Override
    void testCommitOverCommit() throws ExecutionException, InterruptedException {
        // No-op. It's required to test only primary change on cleanup within colocation minimalistic tests set.
    }

    @Override
    void testCommitAlreadyAbortedTx() throws ExecutionException, InterruptedException {
        // No-op. It's required to test only primary change on cleanup within colocation minimalistic tests set.
    }

    @Override
    void testCleanupReplicatedMessage() throws ExecutionException, InterruptedException {
        // No-op. It's required to test only primary change on cleanup within colocation minimalistic tests set.
    }
}
