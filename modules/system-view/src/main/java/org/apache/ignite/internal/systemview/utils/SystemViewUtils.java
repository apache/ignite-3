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
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.systemview.api.ClusterSystemView;
import org.apache.ignite.internal.systemview.api.NodeSystemView;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViewColumn;
import org.apache.ignite.internal.type.BitmaskNativeType;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.NumberNativeType;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.apache.ignite.sql.ColumnType;

/**
 * System views utils.
 */
public class SystemViewUtils {
    private static final int STRING_FIELD_LENGTH = CatalogUtils.DEFAULT_VARLEN_LENGTH;

    /**
     * Creates the {@link BinaryTupleSchema binary tuple schema} for the given system view definition.
     *
     * <p>The schema created will reflect the actual rows emitted by the scan publisher. That is, for
     * node views it will include column representing node name at 0 position.
     *
     * @param view A definition of the view to create schema for.
     * @return A schema representing rows of the given view.
     */
    public static BinaryTupleSchema tupleSchemaForView(SystemView<?> view) {
        int viewColumn = view.columns().size();

        boolean nodeView = view instanceof NodeSystemView;

        Element[] elements = new Element[viewColumn + (nodeView ? 1 : 0)];

        int offset = 0;
        if (nodeView) {
            // for node view we should inject column representing local node name at the very beginning
            // of the tuple
            elements[offset++] = new Element(NativeTypes.stringOf(STRING_FIELD_LENGTH), false);
        }

        for (int i = 0; i < viewColumn; i++) {
            NativeType type = view.columns().get(i).type();

            elements[i + offset] = new Element(type, true);
        }

        return BinaryTupleSchema.create(elements);
    }

    /**
     * Converts {@link SystemViewColumn} to a {@link CreateSystemViewCommand catalog command} to create a system view.
     */
    public static CreateSystemViewCommand toSystemViewCreateCommand(SystemView<?> view) {
        List<ColumnParams> columnParams = new ArrayList<>(view.columns().size());

        SystemViewType viewType;
        if (view instanceof NodeSystemView) {
            columnParams.add(
                    ColumnParams.builder()
                            .name(((NodeSystemView<?>) view).nodeNameColumnAlias())
                            .type(ColumnType.STRING)
                            .length(STRING_FIELD_LENGTH)
                            .nullable(false)
                            .build()
            );

            viewType = SystemViewType.NODE;
        } else {
            assert view instanceof ClusterSystemView : view.getClass().getCanonicalName();

            viewType = SystemViewType.CLUSTER;
        }

        for (SystemViewColumn<?, ?> col : view.columns()) {
            columnParams.add(systemViewColumnToColumnParams(col));
        }

        return CreateSystemViewCommand.builder()
                .name(view.name())
                .columns(columnParams)
                .type(viewType)
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
