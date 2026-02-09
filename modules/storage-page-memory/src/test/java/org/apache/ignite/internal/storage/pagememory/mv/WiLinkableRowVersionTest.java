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
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.util.GridUnsafe;
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
                prevWiLink,
                nextWiLink,
                valueSize,
                null
        );

        String toString = rowVersion.toString();

        assertThat(toString, allOf(
                containsString("rowId=" + rowId),
                containsString("partitionId=" + partitionId),
                containsString("link=" + link),
                containsString("nextLink=" + nextLink),
                containsString("prevWriteIntentLink=" + prevWiLink),
                containsString("nextWriteIntentLink=" + nextWiLink),
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
                prevWiLink, nextWiLink,
                valueSize,
                null
        );

        String toString = rowVersion.toString();

        assertThat(toString, allOf(
                containsString("partitionId=" + partitionId),
                containsString("link=" + link),
                containsString("timestamp=" + timestamp),
                containsString("nextLink=" + nextLink),
                containsString("prevWriteIntentLink=" + prevWiLink),
                containsString("nextWriteIntentLink=" + nextWiLink),
                containsString("valueSize=" + valueSize)
        ));
    }

    @Test
    void nonFragmentedRowHeaderIsReadCorrectly() {
        int partitionId = 0;
        RowId rowId = new RowId(partitionId, 2, 3);
        long link = 5L;
        long nextLink = 7L;
        long nextWiLink = 8L;
        long prevWiLink = 9L;
        int valueSize = 0;

        WiLinkableRowVersion rowVersion = new WiLinkableRowVersion(
                rowId,
                partitionId,
                link,
                null,
                nextLink,
                prevWiLink,
                nextWiLink,
                valueSize,
                null
        );

        ByteBuffer buffer = ByteBuffer.allocateDirect(1024).order(BinaryTuple.ORDER);
        rowVersion.writeRowData(GridUnsafe.bufferAddress(buffer), 0, rowVersion.headerSize(), true);

        WiLinkableWriteIntentReader reader = new WiLinkableWriteIntentReader(link, partitionId);
        reader.readFromPage(GridUnsafe.bufferAddress(buffer), new DataPagePayload(Short.BYTES, 0, NULL_LINK));

        WiLinkableRowVersion reconstructedVersion = reader.createRowVersion(0, null);

        assertThatRowVersionIsAsExpected(reconstructedVersion, rowVersion);
    }

    @Test
    void firstFragmentHeaderIsReadCorrectly() {
        int partitionId = 0;
        RowId rowId = new RowId(partitionId, 2, 3);
        long link = 5L;
        long nextLink = 7L;
        long nextWiLink = 8L;
        long prevWiLink = 9L;
        int valueSize = 0;

        WiLinkableRowVersion rowVersion = new WiLinkableRowVersion(
                rowId,
                partitionId,
                link,
                null,
                nextLink,
                prevWiLink,
                nextWiLink,
                valueSize,
                null
        );

        ByteBuffer buffer = ByteBuffer.allocateDirect(1024).order(BinaryTuple.ORDER);
        rowVersion.writeFragmentData(buffer, 0, rowVersion.headerSize());

        WiLinkableWriteIntentReader reader = new WiLinkableWriteIntentReader(link, partitionId);
        reader.readFromPage(GridUnsafe.bufferAddress(buffer), new DataPagePayload(0, 0, NULL_LINK));

        WiLinkableRowVersion reconstructedVersion = reader.createRowVersion(0, null);

        assertThatRowVersionIsAsExpected(reconstructedVersion, rowVersion);
    }

    private static void assertThatRowVersionIsAsExpected(WiLinkableRowVersion reconstructedVersion, WiLinkableRowVersion expectedVersion) {
        assertThat(reconstructedVersion.link(), is(expectedVersion.link()));
        assertThat(reconstructedVersion.nextLink(), is(expectedVersion.nextLink()));
        assertThat(reconstructedVersion.timestamp(), is(expectedVersion.timestamp()));
        assertThat(reconstructedVersion.valueSize(), is(expectedVersion.valueSize()));
        assertThat(reconstructedVersion.value(), is(expectedVersion.value()));
        assertThat(reconstructedVersion.prevWriteIntentLink(), is(expectedVersion.prevWriteIntentLink()));
        assertThat(reconstructedVersion.nextWriteIntentLink(), is(expectedVersion.nextWriteIntentLink()));
    }
}
