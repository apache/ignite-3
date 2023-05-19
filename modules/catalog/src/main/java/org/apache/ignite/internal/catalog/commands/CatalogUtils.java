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

package org.apache.ignite.internal.catalog.commands;

import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.descriptors.DistributionZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;

/**
 * Catalog utils.
 */
public class CatalogUtils {
    /**
     * Converts CreateTable command params to descriptor.
     *
     * @param id Table id.
     * @param params Parameters.
     * @return Table descriptor.
     */
    public static TableDescriptor fromParams(int id, CreateTableParams params) {
        return new TableDescriptor(id,
                params.tableName(),
                params.columns().stream().map(CatalogUtils::fromParams).collect(Collectors.toList()),
                params.primaryKeyColumns(),
                params.colocationColumns()
        );
    }

    /**
     * Converts DropZone command params to descriptor.
     *
     * @param id Distribution zone id.
     * @param params Parameters.
     * @return Distribution zone descriptor.
     */
    public static DistributionZoneDescriptor fromParams(int id, CreateZoneParams params) {
        return new DistributionZoneDescriptor(
                id,
                params.zoneName(),
                params.partitions(),
                params.replicas()
        );
    }

    private static TableColumnDescriptor fromParams(ColumnParams params) {
        return new TableColumnDescriptor(params.name(), params.type(), params.nullable(), params.defaultValueDefinition());
    }
}
