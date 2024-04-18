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

package org.apache.ignite.internal.table.distributed.schema;

import java.util.Objects;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;

/**
 * Captures a difference between 'old' and 'new' versions of the same column definition.
 */
public class ColumnDefinitionDiff {
    private final CatalogTableColumnDescriptor oldColumn;
    private final CatalogTableColumnDescriptor newColumn;

    public ColumnDefinitionDiff(CatalogTableColumnDescriptor oldColumn, CatalogTableColumnDescriptor newColumn) {
        this.oldColumn = oldColumn;
        this.newColumn = newColumn;
    }

    /**
     * Returns whether nullability has been changed on the column.
     */
    public boolean nullabilityChanged() {
        return oldColumn.nullable() != newColumn.nullable();
    }

    /**
     * Returns whether NOT NULL constraint has been dropped from the column.
     */
    public boolean notNullDropped() {
        return !oldColumn.nullable() && newColumn.nullable();
    }

    /**
     * Returns whether NOT NULL constraint has been added to the column.
     */
    public boolean notNullAdded() {
        return oldColumn.nullable() && !newColumn.nullable();
    }

    /**
     * Returns whether column type (including precision, scale, length) has been changed.
     */
    public boolean typeChanged() {
        return oldColumn.type() != newColumn.type()
                || oldColumn.precision() != newColumn.precision()
                || oldColumn.scale() != newColumn.scale()
                || oldColumn.length() != newColumn.length();
    }

    /**
     * Returns whether type change is supported.
     */
    public boolean typeChangeIsSupported() {
        return CatalogUtils.isColumnTypeChangeSupported(oldColumn, newColumn);
    }

    /**
     * Returns whether the default value has been changed.
     */
    public boolean defaultChanged() {
        return !Objects.equals(oldColumn.defaultValue(), newColumn.defaultValue());
    }
}
