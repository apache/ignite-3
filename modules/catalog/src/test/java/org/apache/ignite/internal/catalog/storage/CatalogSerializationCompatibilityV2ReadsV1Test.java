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

package org.apache.ignite.internal.catalog.storage;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for catalog storage objects. Protocol version 2 reads protocol 1.
 */
public class CatalogSerializationCompatibilityV2ReadsV1Test extends CatalogSerializationCompatibilityTest {

    @Override
    protected int protocolVersion() {
        return 2;
    }

    @Override
    protected int entryVersion() {
        return 1;
    }

    @Override
    protected String dirName() {
        return "serialization_v1";
    }

    @Override
    protected boolean expectExactVersion() {
        return false;
    }

    @Test
    public void alterTableProperties() {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-26632 move to CatalogSerializationCompatibilityV2ReadsV2Test
        List<UpdateEntry> entries = List.of(
                new AlterTablePropertiesEntry(state.id(), null, null),
                new AlterTablePropertiesEntry(state.id(), 1.0d, null),
                new AlterTablePropertiesEntry(state.id(), null, 10L),
                new AlterTablePropertiesEntry(state.id(), 2.0d, 10L)
        );
        checker.compareEntries(entries, "AlterTableProperties", entryVersion());
    }
}
