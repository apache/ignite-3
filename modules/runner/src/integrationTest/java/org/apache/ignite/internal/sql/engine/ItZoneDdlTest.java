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

package org.apache.ignite.internal.sql.engine;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for DDL statements that affect distribution zones.
 */
public class ItZoneDdlTest extends ClusterPerClassIntegrationTest {

    @Test
    public void testCreateIfExists() {
        String zoneName = randomZoneName();

        sql("CREATE ZONE " + zoneName);

        // create zone w/o IF NOT EXIST flag must fail
        assertThrows(Exception.class, () -> sql("CREATE ZONE " + zoneName));

        // create zone with not exist flag
        sql("CREATE ZONE IF NOT EXISTS " + zoneName);
    }

    @Test
    public void testDropZone() {
        String zoneName = randomZoneName();

        sql("CREATE ZONE " + zoneName);
        sql("DROP ZONE " + zoneName);

        // dropping not existing zone w/o IF EXIST flag must fail
        assertThrows(IgniteException.class, () -> sql("DROP ZONE not_existing_" + zoneName));

        // drop with IF EXISTS set
        sql("DROP ZONE IF EXISTS " + zoneName);
    }

    @Test
    public void testRenameZone() {
        String zoneName = randomZoneName();

        sql("CREATE ZONE " + zoneName);
        sql(String.format("ALTER ZONE %s RENAME TO renamed_%s", zoneName, zoneName));
        sql(String.format("ALTER ZONE renamed_%s RENAME TO %s", zoneName, zoneName));

        // renaming not existing zone w/o IF EXIST flag must fail
        assertThrows(IgniteException.class,
                () -> sql(String.format("ALTER ZONE not_existing_%s RENAME TO another_%s", zoneName, zoneName)));

        // rename with IF EXISTS set
        sql(String.format("ALTER ZONE IF EXISTS not_existing_%s RENAME TO another_%s", zoneName, zoneName));
    }

    @Test
    public void testRenameToExistingZone() {
        String zoneName = randomZoneName();
        String anotherZone = randomZoneName() + "_2";

        sql("CREATE ZONE " + zoneName);
        sql("CREATE ZONE " + anotherZone);

        // must fail
        assertThrows(IgniteException.class, () -> sql(String.format("ALTER ZONE %s RENAME TO %s", zoneName, anotherZone)));

        // must fail regardless of IF EXISTS flag because such zone already exists.
        assertThrows(IgniteException.class, () -> sql(String.format("ALTER ZONE IF EXISTS %s RENAME TO %s", zoneName, anotherZone)));
    }

    @Test
    public void testAlterZone() {
        String zoneName = randomZoneName();

        sql("CREATE ZONE " + zoneName);
        sql(String.format("ALTER ZONE %s SET DATA_NODES_AUTO_ADJUST=100", zoneName));

        // altering not existing zone w/o IF EXISTS flag must fail
        assertThrows(IgniteException.class,
                () -> sql(String.format("ALTER ZONE not_existing_%s SET DATA_NODES_AUTO_ADJUST=200", zoneName)));

        // alter with IF EXISTS set
        sql(String.format("ALTER ZONE IF EXISTS not_existing_%s SET DATA_NODES_AUTO_ADJUST=200", zoneName));
    }

    private static String randomZoneName() {
        return "test_zone" + System.currentTimeMillis();
    }
}
