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

package org.apache.ignite.internal.sql.engine.prepare;

import static org.apache.calcite.util.Static.RESOURCE;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.AliasNamespace;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;

/**
 * Alias namespace that properly handles `system` (aka {@link ColumnDescriptor#hidden() hidden}) columns during renaming.
 */
public class IgniteAliasNamespace extends AliasNamespace {
    IgniteAliasNamespace(
            SqlValidatorImpl validator, SqlCall call, SqlNode enclosingNode
    ) {
        super(validator, call, enclosingNode);
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        List<SqlNode> operands = call.getOperandList();

        if (operands.size() == 2) {
            return super.validateImpl(targetRowType);
        }

        // Alias is 'AS t (c0, ..., cN)'
        SqlValidatorNamespace childNs = validator.getNamespace(operands.get(0));

        assert childNs != null : "Alias in FROM list must have a namespace";

        SqlValidatorTable table = childNs.getTable();

        if (table == null) {
            // Not all namespaces derived from a table (for example, the one derived from sub-query).
            // In this case let's fallback to original validation, because logic below is meant to
            // handle `system` columns which belongs solely to the IgniteTable.
            return super.validateImpl(targetRowType);
        }

        List<SqlNode> columnNames = Util.skip(operands, 2);
        List<String> nameList = SqlIdentifier.simpleNames(columnNames);

        int i = Util.firstDuplicate(nameList);
        if (i >= 0) {
            SqlIdentifier id = (SqlIdentifier) columnNames.get(i);
            throw validator.newValidationError(id,
                    RESOURCE.aliasListDuplicate(id.getSimple()));
        }

        TableDescriptor descriptor = table.unwrap(TableDescriptor.class);

        assert descriptor != null;

        RelDataType rowTypeSansHidden = descriptor.rowTypeSansHidden();
        // At the moment, only system columns are hidden, and user must provide
        // aliases for all non-system columns.
        if (columnNames.size() != rowTypeSansHidden.getFieldList().size()) {
            // Position error over all column names
            SqlNode node = operands.size() == 3
                    ? operands.get(2)
                    : new SqlNodeList(columnNames, SqlParserPos.sum(columnNames));

            throw validator.newValidationError(node,
                    RESOURCE.aliasListDegree(rowTypeSansHidden.getFieldCount(),
                            getString(rowTypeSansHidden), columnNames.size()));
        }

        // Although this may look like proper way to address `system` columns, in fact it's not:
        //     1) Calcite has different notion of `system` columns. First of all, all implementations
        //     of namespace just propagate call to getRowType() method. Second, system columns are
        //     registered only when converter meets LogicalJoin.
        //
        //     2) Row type returned from this method will be used to resolve columns. Thus, if we skip
        //     `system` columns validator won't be able to resolve any reference of such columns (given
        //     that they are registered only in case of joins)
        RelDataType rowType = childNs.getRowTypeSansSystemColumns();

        assert rowType != null;

        int fieldsCount = rowType.getFieldCount();
        Mapping mapping = Mappings.create(MappingType.PARTIAL_FUNCTION, fieldsCount, nameList.size());
        int idx = 0;
        for (ColumnDescriptor column : descriptor) {
            if (!column.hidden()) {
                mapping.set(column.logicalIndex(), idx++);
            }
        }

        RelDataType aliasedType = validator.getTypeFactory().builder()
                .addAll(
                        Util.transform(
                                rowType.getFieldList(),
                                f -> {
                                    int index = mapping.getTargetOpt(f.getIndex());
                                    String name = index == -1 ? f.getName() : nameList.get(index);

                                    return Pair.of(name, f.getType());
                                }
                        )
                )
                .kind(rowType.getStructKind())
                .build();

        return validator.getTypeFactory()
                .createTypeWithNullability(aliasedType, rowType.isNullable());
    }

    @SuppressWarnings("MethodOverridesInaccessibleMethodOfSuper")
    private static String getString(RelDataType rowType) {
        StringBuilder buf = new StringBuilder();
        buf.append('(');
        for (RelDataTypeField field : rowType.getFieldList()) {
            if (field.getIndex() > 0) {
                buf.append(", ");
            }
            buf.append('\'');
            buf.append(field.getName());
            buf.append('\'');
        }
        buf.append(')');
        return buf.toString();
    }
}
