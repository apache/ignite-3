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

package org.apache.ignite.internal.schema.marshaller.asm;

import static org.apache.ignite.internal.marshaller.ValidationUtils.validateColumnType;

import com.facebook.presto.bytecode.Access;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.marshaller.BinaryMode;
import org.apache.ignite.internal.marshaller.MarshallerColumn;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.table.mapper.PojoMapper;

/**
 * Generates marshaller methods code.
 */
class ObjectMarshallerCodeGenerator implements MarshallerCodeGenerator {
    /** Target class. */
    private final Class<?> targetClass;

    /** Mapped columns. */
    private final MarshallerColumn[] columns;

    /** Object field access expression generators. */
    private final ColumnAccessCodeGenerator[] columnAccessors;

    ObjectMarshallerCodeGenerator(
            MarshallerColumn[] marshallerColumns,
            PojoMapper<?> mapper,
            int firstColIdx
    ) {
        this.columns = marshallerColumns;
        this.targetClass = mapper.targetType();
        columnAccessors = new ColumnAccessCodeGenerator[columns.length];

        Map<String, Field> flds = new HashMap<>();
        for (String fldName : mapper.fields()) {
            try {
                Field field = mapper.targetType().getDeclaredField(fldName);
                flds.put(fldName.toUpperCase(), field);
            } catch (NoSuchFieldException e) {
                throw new IllegalArgumentException(
                        "Field " + fldName + " is returned from mapper of type " + mapper.getClass().getName()
                                + ", but is not present in target class " + mapper.targetType().getName(), e);
            }
        }

        if (flds.size() > columns.length) {
            for (MarshallerColumn col : columns) {
                flds.remove(col.name());
            }

            var fldNames = flds.values().stream().map(Field::getName).sorted().collect(Collectors.toList());

            throw new IllegalArgumentException(
                    "Fields " + fldNames + " of type " + targetClass.getName() + " are not mapped to columns");
        }

        for (int i = 0; i < columns.length; i++) {
            MarshallerColumn column = columns[i];

            Field field = flds.get(column.name());

            if (field == null) {
                throw new IllegalArgumentException("No field found for column " + column.name());
            }

            validateColumnType(column, field.getType());

            columnAccessors[i] = ColumnAccessCodeGenerator.createAccessor(
                    BinaryMode.forClass(field.getType()),
                    field.getName(),
                    i + firstColIdx);
        }
    }

    /** {@inheritDoc} */
    @Override
    public BytecodeNode getValue(ParameterizedType marshallerClass, Variable obj,
            int i) {
        final ColumnAccessCodeGenerator columnAccessor = columnAccessors[i];

        // We need to skip casting to the mapped type in order to check for null even for primitive types
        return BytecodeExpressions.getStatic(marshallerClass, "FIELD_HANDLER_" + columnAccessor.columnIdx(),
                        ParameterizedType.type(VarHandle.class))
                .invoke("get", Object.class, obj);
    }

    /** {@inheritDoc} */
    @Override
    public BytecodeBlock marshallObject(ParameterizedType marshallerClass, Variable asm, Variable obj) {
        final BytecodeBlock block = new BytecodeBlock();

        for (int i = 0; i < columns.length; i++) {
            final ColumnAccessCodeGenerator columnAccessor = columnAccessors[i];

            final BytecodeExpression fld = BytecodeExpressions.getStatic(
                            marshallerClass,
                            "FIELD_HANDLER_" + columnAccessor.columnIdx(),
                            ParameterizedType.type(VarHandle.class)
                    )
                    .invoke("get", columnAccessor.mappedType(), obj);

            block.append(asm.invoke(
                    columnAccessor.writeMethodName(),
                    RowAssembler.class,
                    Collections.singletonList(columnAccessor.mappedType()),
                    fld.cast(columnAccessor.mappedType())));
        }

        return block;
    }

    /** {@inheritDoc} */
    @Override
    public BytecodeBlock unmarshallObject(ParameterizedType marshallerClass, Variable row, Variable objVar, Variable objFactory) {
        final BytecodeBlock block = new BytecodeBlock();

        block.append(objVar.set(objFactory.invoke("create", Object.class)));

        for (int i = 0; i < columns.length; i++) {
            final ColumnAccessCodeGenerator columnAccessor = columnAccessors[i];

            final BytecodeExpression val = row.invoke(
                    columnAccessor.readMethodName(),
                    columnAccessor.mappedType(),
                    BytecodeExpressions.constantInt(columnAccessor.columnIdx())
            );

            block.append(BytecodeExpressions.getStatic(marshallerClass, "FIELD_HANDLER_" + columnAccessor.columnIdx(),
                            ParameterizedType.type(VarHandle.class))
                    .invoke("set", void.class, objVar, val)
            );
        }

        return block;
    }

    /** {@inheritDoc} */
    @Override
    public void initStaticHandlers(ClassDefinition classDef) {
        final MethodDefinition init = classDef.getClassInitializer();
        final Variable lookup = init.getScope().createTempVariable(MethodHandles.Lookup.class);
        Variable targetClassVar = init.getScope().createTempVariable(Class.class);

        final BytecodeBlock body = init.getBody().append(
                targetClassVar.set(
                        BytecodeExpressions.invokeStatic(Class.class, "forName", Class.class,
                                BytecodeExpressions.constantString(targetClass.getName()))
                ));

        body.append(
                lookup.set(
                        BytecodeExpressions.invokeStatic(
                                MethodHandles.class,
                                "privateLookupIn",
                                MethodHandles.Lookup.class,
                                targetClassVar,
                                BytecodeExpressions.invokeStatic(MethodHandles.class, "lookup", MethodHandles.Lookup.class))
                ));

        for (int i = 0; i < columnAccessors.length; i++) {
            final FieldDefinition fld = classDef.declareField(EnumSet.of(Access.PRIVATE, Access.STATIC, Access.FINAL),
                    "FIELD_HANDLER_" + columnAccessors[i].columnIdx(), VarHandle.class);

            body.append(
                    BytecodeExpressions.setStatic(fld, lookup.invoke(
                            "findVarHandle",
                            VarHandle.class,
                            targetClassVar,
                            BytecodeExpressions.constantString(columnAccessors[i].fieldName()),
                            BytecodeExpressions.constantClass(columnAccessors[i].mappedType())
                    ))
            );
        }
    }
}
