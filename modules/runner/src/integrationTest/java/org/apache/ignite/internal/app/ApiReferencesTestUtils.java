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

package org.apache.ignite.internal.app;

import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.table.Tuple;

/**
 * Utils and constants for tests relating to testing restart-proof API references.
 */
class ApiReferencesTestUtils {
    static final String TEST_TABLE_NAME = "test";
    static final Tuple KEY_TUPLE = Tuple.create().set("id", 1);
    static final Tuple VALUE_TUPLE = Tuple.create().set("val", "one");
    static final Tuple FULL_TUPLE = Tuple.create().set("id", 1).set("val", "one");

    static final String SELECT_IDS_QUERY = "SELECT id FROM " + TEST_TABLE_NAME;
    static final String UPDATE_QUERY = "UPDATE " + TEST_TABLE_NAME + " SET val = val WHERE id = ?";

    static TableDefinition tableDefinition() {
        return TableDefinition.builder("def_table")
                .record(Pojo.class)
                .ifNotExists()
                .build();
    }

    static ZoneDefinition zoneDefinition() {
        return ZoneDefinition.builder("zone")
                .storageProfiles(CatalogService.DEFAULT_STORAGE_PROFILE)
                .ifNotExists()
                .build();
    }
}
