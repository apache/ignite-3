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

package org.apache.ignite.internal.streamer;

import org.apache.ignite.Ignite;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration test for server-side data streamer API.
 */
// Disabling thread assertions as DataStreamer uses common pool on which ReplicaManager executes its requests.
public class ItServerDataStreamerTest extends ItAbstractDataStreamerTest {
    @Override
    Ignite ignite() {
        return CLUSTER.aliveNode();
    }

    @Test
    @Override
    @Disabled("IGNITE-22285 Embedded Data Streamer with Receiver")
    public void testWithReceiver() {
        super.testWithReceiver();
    }

    @Test
    @Override
    @Disabled("IGNITE-22285 Embedded Data Streamer with Receiver")
    public void testReceiverException(boolean async) {
        super.testReceiverException(async);
    }

    @Test
    @Override
    @Disabled("IGNITE-22285 Embedded Data Streamer with Receiver")
    public void testReceivedIsExecutedOnTargetNode() {
        super.testReceivedIsExecutedOnTargetNode();
    }
}
