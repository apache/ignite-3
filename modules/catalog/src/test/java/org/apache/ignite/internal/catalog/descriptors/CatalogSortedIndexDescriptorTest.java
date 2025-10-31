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

package org.apache.ignite.internal.catalog.descriptors;

import static org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptorTest.tableDescriptor;
import static org.apache.ignite.internal.util.CollectionUtils.view;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import org.junit.jupiter.api.Test;

class CatalogSortedIndexDescriptorTest {
    @Test
    void toStringContainsTypeAndFields() {
        var descriptor = new CatalogSortedIndexDescriptor(1, "index1", 2, false, List.of());

        String toString = descriptor.toString();

        assertThat(toString, startsWith("CatalogSortedIndexDescriptor ["));
        assertThat(toString, containsString("id=1"));
        assertThat(toString, containsString("name=index1"));
        assertThat(toString, containsString("tableId=2"));
        assertThat(toString, containsString("status=REGISTERED"));
    }

    @Test
    void upgradeDescriptorTest() {
        int tableId = 2;
        @SuppressWarnings("removal")
        List<CatalogIndexColumnDescriptor> columns = List.of(
                new CatalogIndexColumnDescriptor("val_1", CatalogColumnCollation.ASC_NULLS_LAST),
                new CatalogIndexColumnDescriptor("val_2", CatalogColumnCollation.ASC_NULLS_LAST)
        );
        var original = new CatalogSortedIndexDescriptor(
                1, "index1", tableId, false, CatalogIndexStatus.REGISTERED, columns, false
        );

        {
            CatalogSortedIndexDescriptor resulting = original.upgradeIfNeeded(tableDescriptor(tableId, "id", "val_1", "val_2"));

            assertThat(view(resulting.columns(), CatalogIndexColumnDescriptor::columnId), is(List.of(1, 2)));
            assertThat(resulting.id(), is(original.id()));
            assertThat(resulting.name(), is(original.name()));
            assertThat(resulting.tableId(), is(original.tableId()));
            assertThat(resulting.unique(), is(original.unique()));
            assertThat(resulting.status(), is(original.status()));
            assertThat(resulting.isCreatedWithTable(), is(original.isCreatedWithTable()));
        }

        {
            CatalogSortedIndexDescriptor resulting = original.upgradeIfNeeded(tableDescriptor(tableId, "id", "val_2", "val_1"));

            assertThat(view(resulting.columns(), CatalogIndexColumnDescriptor::columnId), is(List.of(2, 1)));
            assertThat(resulting.id(), is(original.id()));
            assertThat(resulting.name(), is(original.name()));
            assertThat(resulting.tableId(), is(original.tableId()));
            assertThat(resulting.unique(), is(original.unique()));
            assertThat(resulting.status(), is(original.status()));
            assertThat(resulting.isCreatedWithTable(), is(original.isCreatedWithTable()));
        }

        {
            CatalogTableDescriptor table = tableDescriptor(tableId, "id", "val_1", "val_2");

            CatalogSortedIndexDescriptor afterUpgrade = original.upgradeIfNeeded(table);

            assertSame(afterUpgrade, afterUpgrade.upgradeIfNeeded(table));
        }
    }
}
