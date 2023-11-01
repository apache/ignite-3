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

package org.apache.ignite.distributed;

import static org.mockito.Mockito.mock;

import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link InternalTable#scan(int, org.apache.ignite.internal.tx.InternalTransaction)}.
 */
@ExtendWith(MockitoExtension.class)
public class ItInternalTableReadOnlyScanTest extends ItAbstractInternalTableScanTest {
    @Override
    protected Publisher<BinaryRow> scan(int part, InternalTransaction tx) {
        return internalTbl.scan(part, internalTbl.CLOCK.now(), mock(ClusterNode.class));
    }

    // TODO: IGNITE-17666 Use super test as is.
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17666")
    @Override
    public void testExceptionRowScanCursorHasNext() throws Exception {
        super.testExceptionRowScanCursorHasNext();
    }
}
