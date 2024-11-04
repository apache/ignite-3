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

package org.apache.ignite.internal.table.distributed.index;

import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.BUILDING;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.READ_ONLY;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.REGISTERED;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.REMOVED;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.STOPPING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import java.util.Map;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class IndexMetaSerializerTest {
    private static final String V1_SERIALIZED_BASE64 = "Ae++Q+kH0Q+5F6EfBmluZGV4BgcEBQAAAAAAAJEDAgMAAAAAAADJAQYHAAAAAAAA2QQFBgAAAAAAAPU"
            + "DAwQAAAAAAACtAgECAAAAAAAAZQ==";

    private final IndexMetaSerializer serializer = new IndexMetaSerializer();

    @Test
    void serializationAndDeserialization() {
        IndexMeta originalMeta = new IndexMeta(1000, 2000, 3000, 4000, "index", READ_ONLY, originalStatusChanges());

        byte[] bytes = VersionedSerialization.toBytes(originalMeta, serializer);
        IndexMeta restoredMeta = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMeta.catalogVersion(), is(1000));
        assertThat(restoredMeta.indexId(), is(2000));
        assertThat(restoredMeta.tableId(), is(3000));
        assertThat(restoredMeta.tableVersion(), is(4000));
        assertThat(restoredMeta.indexName(), is("index"));
        assertThat(restoredMeta.status(), is(READ_ONLY));
        assertThat(restoredMeta.statusChanges(), equalTo(originalStatusChanges()));
    }

    private static Map<MetaIndexStatus, MetaIndexStatusChange> originalStatusChanges() {
        return Map.of(
                REGISTERED, new MetaIndexStatusChange(1, 100),
                BUILDING, new MetaIndexStatusChange(2, 200),
                AVAILABLE, new MetaIndexStatusChange(3, 300),
                STOPPING, new MetaIndexStatusChange(4, 400),
                REMOVED, new MetaIndexStatusChange(5, 500),
                READ_ONLY, new MetaIndexStatusChange(6, 600)
        );
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(V1_SERIALIZED_BASE64);
        IndexMeta restoredMeta = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMeta.catalogVersion(), is(1000));
        assertThat(restoredMeta.indexId(), is(2000));
        assertThat(restoredMeta.tableId(), is(3000));
        assertThat(restoredMeta.tableVersion(), is(4000));
        assertThat(restoredMeta.indexName(), is("index"));
        assertThat(restoredMeta.status(), is(READ_ONLY));
        assertThat(restoredMeta.statusChanges(), equalTo(originalStatusChanges()));
    }

    @SuppressWarnings("unused")
    private String v1Base64() {
        IndexMeta originalMeta = new IndexMeta(1000, 2000, 3000, 4000, "index", READ_ONLY, originalStatusChanges());

        byte[] v1Bytes = VersionedSerialization.toBytes(originalMeta, serializer);
        return Base64.getEncoder().encodeToString(v1Bytes);
    }
}
