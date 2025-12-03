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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.storage.RowId;
import org.junit.jupiter.api.Test;

class WiLinkableRowVersionTest {
    @Test
    void toStringContainsAllFieldsForWriteIntent() {
        int partitionId = 4;
        RowId rowId = new RowId(partitionId, 2, 3);
        long link = 5L;
        long nextLink = 7L;
        long nextWiLink = 8L;
        long prevWiLink = 9L;
        int valueSize = 10;

        WiLinkableRowVersion rowVersion = new WiLinkableRowVersion(
                rowId,
                partitionId,
                link,
                null,
                nextLink,
                nextWiLink,
                prevWiLink,
                valueSize,
                null
        );

        String toString = rowVersion.toString();

        assertThat(toString, allOf(
                containsString("rowId=" + rowId),
                containsString("partitionId=" + partitionId),
                containsString("link=" + link),
                containsString("nextLink=" + nextLink),
                containsString("nextWriteIntentLink=" + nextWiLink),
                containsString("prevWriteIntentLink=" + prevWiLink),
                containsString("valueSize=" + valueSize)
        ));
    }

    @Test
    void toStringContainsAllFieldsForCommittedVersion() {
        int partitionId = 4;
        long link = 5L;
        HybridTimestamp timestamp = hybridTimestamp(6L);
        long nextLink = 7L;
        long nextWiLink = 8L;
        long prevWiLink = 9L;
        int valueSize = 10;

        WiLinkableRowVersion rowVersion = new WiLinkableRowVersion(
                null,
                partitionId,
                link,
                timestamp,
                nextLink,
                nextWiLink,
                prevWiLink,
                valueSize,
                null
        );

        String toString = rowVersion.toString();

        assertThat(toString, allOf(
                containsString("partitionId=" + partitionId),
                containsString("link=" + link),
                containsString("timestamp=" + timestamp),
                containsString("nextLink=" + nextLink),
                containsString("nextWriteIntentLink=" + nextWiLink),
                containsString("prevWriteIntentLink=" + prevWiLink),
                containsString("valueSize=" + valueSize)
        ));
    }
}
