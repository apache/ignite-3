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

package org.apache.ignite.internal.systemview.utils;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.internal.catalog.commands.CreateSystemViewCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.systemview.NodeSystemView;
import org.apache.ignite.internal.systemview.SystemView;
import org.apache.ignite.internal.systemview.SystemViewColumn;
import org.apache.ignite.sql.ColumnType;

/**
 * System views utils.
 */
public class SystemViewUtils {
    /** Default decimal precision. */
    // TODO Remove after https://issues.apache.org/jira/browse/IGNITE-20513
    private static final int DEFAULT_DECIMAL_PRECISION = 19;

    /**
     * Converts {@link SystemViewColumn} to a {@link CreateSystemViewCommand catalog command} to create a system view.
     */
    public static CreateSystemViewCommand toSystemViewCreateCommand(SystemView<?> view) {
        List<ColumnParams> columnParams = new ArrayList<>(view.columns().size());

        if (view.type() == SystemViewType.LOCAL) {
            columnParams.add(
                    ColumnParams.builder()
                            .name(((NodeSystemView<?>) view).nodeNameColumnAlias())
                            .type(ColumnType.STRING)
                            .length(CatalogUtils.DEFAULT_VARLEN_LENGTH)
                            .build()
            );
        }

        for (SystemViewColumn<?, ?> col : view.columns()) {
            columnParams.add(systemViewColumnToColumnParams(col));
        }

        return CreateSystemViewCommand.builder()
                .name(view.name())
                .columns(columnParams)
                .type(view.type())
                .build();
    }

    private static ColumnParams systemViewColumnToColumnParams(SystemViewColumn<?, ?> column) {
        NativeTypeSpec typeSpec = NativeTypeSpec.fromClass(column.type());

        Builder builder = ColumnParams.builder()
                .name(column.name())
                .type(typeSpec.asColumnType());

        // TODO Don't use defaults after https://issues.apache.org/jira/browse/IGNITE-20513
        switch (typeSpec) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case UUID:
            case BOOLEAN:
                break;
            case NUMBER:
                builder.precision(DEFAULT_DECIMAL_PRECISION);
                break;
            case DECIMAL:
                builder.precision(DEFAULT_DECIMAL_PRECISION);
                builder.scale(CatalogUtils.DEFAULT_SCALE);
                break;
            case STRING:
            case BYTES:
            case BITMASK:
                builder.length(CatalogUtils.DEFAULT_VARLEN_LENGTH);
                break;
            case TIME:
                builder.precision(CatalogUtils.DEFAULT_TIME_PRECISION);
                break;
            case DATETIME:
            case TIMESTAMP:
                builder.precision(CatalogUtils.DEFAULT_TIMESTAMP_PRECISION);
                break;
            default:
                throw new IllegalArgumentException("Unsupported native type: " + typeSpec);
        }

        return builder.build();
    }
}
