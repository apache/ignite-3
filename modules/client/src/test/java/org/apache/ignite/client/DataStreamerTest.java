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

package org.apache.ignite.client;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Data streamer test.
 */
public class DataStreamerTest extends AbstractClientTableTest {
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void testBasicStreaming(int batchSize) {
        RecordView<Tuple> view = this.defaultTable().recordView();

        var publisher = new SubmissionPublisher<Tuple>();
        CompletableFuture<Void> fut = view.streamData(publisher, new DataStreamerOptions().batchSize(batchSize));

        publisher.submit(tuple(1L, "foo"));
        publisher.submit(tuple(2L, "bar"));

        publisher.close();
        fut.orTimeout(1, TimeUnit.SECONDS).join();

        assertNotNull(view.get(null, tupleKey(1L)));
        assertNotNull(view.get(null, tupleKey(2L)));
        assertNull(view.get(null, tupleKey(3L)));

        assertEquals("bar", view.get(null, tupleKey(2L)).stringValue("name"));
    }

    @Test
    public void testAutoFlushByTimer() throws InterruptedException {
        RecordView<Tuple> view = this.defaultTable().recordView();

        var publisher = new SubmissionPublisher<Tuple>();
        view.streamData(publisher, new DataStreamerOptions().autoFlushFrequency(100));

        publisher.submit(tuple(1L, "foo"));
        assertTrue(waitForCondition(() -> view.get(null, tupleKey(1L)) != null, 1000));
    }

    @Test
    public void testBackPressure() {
        assert false;
    }

    @Test
    public void testPartitionAwareness() {
        // TODO: See how PartitionAwarenessTest is implemented using setDataAccessListener
        assert false;
    }

    @Test
    public void testRetry() {
        assert false;
    }
}
