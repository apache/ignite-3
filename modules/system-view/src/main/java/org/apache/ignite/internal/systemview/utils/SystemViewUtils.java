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
import org.apache.ignite.internal.schema.BitmaskNativeType;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NumberNativeType;
import org.apache.ignite.internal.schema.TemporalNativeType;
import org.apache.ignite.internal.schema.VarlenNativeType;
import org.apache.ignite.internal.systemview.NodeSystemView;
import org.apache.ignite.internal.systemview.SystemView;
import org.apache.ignite.internal.systemview.SystemViewColumn;
import org.apache.ignite.sql.ColumnType;

/**
 * System views utils.
 */
public class SystemViewUtils {
    private static final int NODE_NAME_FIELD_LENGTH = CatalogUtils.DEFAULT_VARLEN_LENGTH;

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
                            .length(NODE_NAME_FIELD_LENGTH)
                            .nullable(false)
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
        NativeType type = column.type();

        NativeTypeSpec typeSpec = type.spec();

        Builder builder = ColumnParams.builder()
                .name(column.name())
                .type(typeSpec.asColumnType())
                .nullable(true);

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
                assert type instanceof NumberNativeType : type.getClass().getCanonicalName();

                builder.precision(((NumberNativeType) type).precision());
                break;
            case DECIMAL:
                assert type instanceof DecimalNativeType : type.getClass().getCanonicalName();

                builder.precision(((DecimalNativeType) type).precision());
                builder.scale(((DecimalNativeType) type).scale());
                break;
            case STRING:
            case BYTES:
                assert type instanceof VarlenNativeType : type.getClass().getCanonicalName();

                builder.length(((VarlenNativeType) type).length());
                break;
            case BITMASK:
                assert type instanceof BitmaskNativeType : type.getClass().getCanonicalName();

                builder.length(((BitmaskNativeType) type).bits());
                break;
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                assert type instanceof TemporalNativeType : type.getClass().getCanonicalName();

                builder.precision(((TemporalNativeType) type).precision());
                break;
            default:
                throw new IllegalArgumentException("Unsupported native type: " + typeSpec);
        }

        return builder.build();
    }
}
