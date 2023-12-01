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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import org.junit.jupiter.api.Test;

class QueryCatalogVersionsTest {
    @Test
    void maxVersionChoosesBaseIfNoOverridesExist() {
        QueryCatalogVersions versions = new QueryCatalogVersions(10, Map.of());

        assertThat(versions.maxVersion(), is(10));
    }

    @Test
    void maxVersionChoosesMaxOverrideIfExists() {
        QueryCatalogVersions versions = new QueryCatalogVersions(10, Map.of(1, 11, 2, 12));

        assertThat(versions.maxVersion(), is(12));
    }

    @Test
    void catalogVersionForTableChoosesBaseVersionWhenNoOverrideExists() {
        QueryCatalogVersions versions = new QueryCatalogVersions(10, Map.of());

        assertThat(versions.catalogVersionForTable(1), is(10));
    }

    @Test
    void catalogVersionForTableChoosesBaseVersionWhenMatchingOverrideExists() {
        QueryCatalogVersions versions = new QueryCatalogVersions(10, Map.of(1, 11));

        assertThat(versions.catalogVersionForTable(1), is(11));
    }

    @Test
    void catalogVersionForTableChoosesBaseVersionWhenAnotherOverrideExists() {
        QueryCatalogVersions versions = new QueryCatalogVersions(10, Map.of(2, 12));

        assertThat(versions.catalogVersionForTable(1), is(10));
    }
}
