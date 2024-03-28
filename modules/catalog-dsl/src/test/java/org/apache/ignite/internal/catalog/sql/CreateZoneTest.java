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

package org.apache.ignite.internal.catalog.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.catalog.Options;
import org.junit.jupiter.api.Test;

class CreateZoneTest {
    @Test
    void testIfNotExists() {
        String sql = createZone().ifNotExists().name("zone1").toSqlString();
        assertThat(sql, is("CREATE ZONE IF NOT EXISTS zone1;"));

        sql = createZoneQuoted().ifNotExists().name("zone1").toSqlString();
        assertThat(sql, is("CREATE ZONE IF NOT EXISTS \"zone1\";"));
    }

    @Test
    void testNames() {
        String sql = createZone().name("public", "zone1").toSqlString();
        assertThat(sql, is("CREATE ZONE public.zone1;"));

        // quote identifiers
        sql = createZoneQuoted().name("public", "zone1").toSqlString();
        assertThat(sql, is("CREATE ZONE \"public\".\"zone1\";"));
    }

    @Test
    void testWithOptions() {
        String sql = createZone().name("zone1").partitions(1).toSqlString();
        assertThat(sql, is("CREATE ZONE zone1 WITH PARTITIONS=1;"));

        sql = createZone().name("zone1").partitions(1).replicas(1).toSqlString();
        assertThat(sql, is("CREATE ZONE zone1 WITH PARTITIONS=1, REPLICAS=1;"));

        // quote identifiers
        sql = createZoneQuoted().name("zone1").partitions(1).toSqlString();
        assertThat(sql, is("CREATE ZONE \"zone1\" WITH PARTITIONS=1;"));

        sql = createZoneQuoted().name("zone1").partitions(1).replicas(1).toSqlString();
        assertThat(sql, is("CREATE ZONE \"zone1\" WITH PARTITIONS=1, REPLICAS=1;"));

        sql = createZoneQuoted().name("zone1").storageProfiles("profile1,profile2").toSqlString();
        assertThat(sql, is("CREATE ZONE \"zone1\" WITH STORAGE_PROFILES='profile1,profile2';"));
    }

    private static CreateZoneImpl createZone() {
        return createZone(Options.DEFAULT);
    }

    private static CreateZoneImpl createZone(Options options) {
        return new CreateZoneImpl(null, options);
    }

    private static CreateZoneImpl createZoneQuoted() {
        return createZone(Options.builder().quoteIdentifiers().build());
    }
}
