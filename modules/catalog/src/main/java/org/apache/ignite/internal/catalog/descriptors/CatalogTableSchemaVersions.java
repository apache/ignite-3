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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Class that holds a list of table version descriptors.
 */
public class CatalogTableSchemaVersions implements Serializable {
    private static final long serialVersionUID = 4353352473287209850L;

    /**
     * Descriptor of a single table version.
     */
    public static class TableVersion implements Serializable {
        private static final long serialVersionUID = -5185852983239967322L;

        private final List<CatalogTableColumnDescriptor> columns;

        TableVersion(List<CatalogTableColumnDescriptor> columns) {
            this.columns = columns;
        }

        public List<CatalogTableColumnDescriptor> columns() {
            return Collections.unmodifiableList(columns);
        }
    }

    private final int base;
    private final TableVersion[] versions;

    /**
     * Constructor.
     *
     * @param versions Array of table versions.
     */
    CatalogTableSchemaVersions(TableVersion... versions) {
        this(CatalogTableDescriptor.INITIAL_TABLE_VERSION, versions);
    }

    private CatalogTableSchemaVersions(int base, TableVersion... versions) {
        this.base = base;
        this.versions = versions;
    }

    /**
     * Returns earliest known table version.
     */
    public int earliestVersion() {
        return base;
    }

    /**
     * Returns latest known table version.
     */
    public int latestVersion() {
        return base + versions.length - 1;
    }

    /**
     * Returns an existing table version, or {@code null} if it's not found.
     */
    public @Nullable TableVersion get(int version) {
        if (version < base || version >= base + versions.length) {
            return null;
        }

        return versions[version - base];
    }

    /**
     * Creates a new instance of {@link CatalogTableSchemaVersions} with one new version appended.
     */
    public CatalogTableSchemaVersions append(TableVersion tableVersion, int version) {
        assert version == latestVersion() + 1;

        return new CatalogTableSchemaVersions(base, ArrayUtils.concat(versions, tableVersion));
    }
}
