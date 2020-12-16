/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema.marshaller.generator;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.TypeSpec;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import javax.lang.model.element.Modifier;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.marshaller.Serializer;

/**
 * Generates {@link Serializer} methods code.
 */
class ObjectMarshallerCodeGenerator implements MarshallerCodeGenerator {
    /** Target object factory var. */
    private final String objectFactoryVar;

    /** Target class. */
    private final Class<?> tClass;

    /** Mapped columns. */
    private final Columns columns;

    /** Object field access expression generators. */
    private final TupleColumnAccessCodeGenerator[] columnAccessessors;

    /**
     * Constructor.
     *
     * @param tClass Target object class.
     * @param objectFactoryVar Target object factory var.
     * @param columns Column object is mapped to.
     * @param firstColIdx Column index offset.
     */
    public ObjectMarshallerCodeGenerator(Class<?> tClass, String objectFactoryVar, Columns columns, int firstColIdx) {
        this.objectFactoryVar = objectFactoryVar;
        this.tClass = tClass;

        this.columns = columns;
        columnAccessessors = new TupleColumnAccessCodeGenerator[this.columns.length()];
        try {
            for (int i = 0; i < columns.length(); i++) {
                final Field field = tClass.getDeclaredField(columns.column(i).name());

                columnAccessessors[i] = TupleColumnAccessCodeGenerator.createAccessor(MarshallerUtil.mode(field.getType()), i + firstColIdx);
            }
        }
        catch (NoSuchFieldException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /** {@inheritDoc} */
    @Override public Class<?> getClazz() {
        return tClass;
    }

    /** {@inheritDoc} */
    @Override public boolean isSimpleType() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public CodeBlock unmarshallObjectCode(String tupleExpr) {
        final CodeBlock.Builder builder = CodeBlock.builder()
            .addStatement("$T obj = $L.create()", tClass, objectFactoryVar);

        for (int i = 0; i < columnAccessessors.length; i++)
            builder.addStatement("FIELD_HANDLE_$L.set(obj, $L)", columnAccessessors[i].columnIdx(), columnAccessessors[i].read(tupleExpr));

        builder.addStatement("return obj");
        return builder.build();
    }

    /** {@inheritDoc} */
    @Override public CodeBlock getValueCode(String objVar, int i) {
        return CodeBlock.of("FIELD_HANDLE_$L.get($L)", columnAccessessors[i].columnIdx(), objVar);
    }

    /** {@inheritDoc} */
    @Override public CodeBlock marshallObjectCode(String asm, String objVar) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        for (int i = 0; i < columnAccessessors.length; i++)
            builder.add(columnAccessessors[i].write(asm, getValueCode(objVar, i).toString()));

        return builder.build();
    }

    /** {@inheritDoc} */
    @Override public void initStaticHandlers(TypeSpec.Builder builder, CodeBlock.Builder staticBuilder) {
        for (int i = 0; i < columnAccessessors.length; i++) {
            builder.addField(FieldSpec.builder(
                VarHandle.class,
                CodeBlock.of("FIELD_HANDLE_$L", columnAccessessors[i].columnIdx()).toString(),
                Modifier.PRIVATE,
                Modifier.FINAL,
                Modifier.STATIC)
                .build());

            staticBuilder.addStatement("FIELD_HANDLE_$L = lookup.unreflectVarHandle($T.class.getDeclaredField($S))",
                columnAccessessors[i].columnIdx(), tClass, columns.column(i).name());
        }
    }
}
