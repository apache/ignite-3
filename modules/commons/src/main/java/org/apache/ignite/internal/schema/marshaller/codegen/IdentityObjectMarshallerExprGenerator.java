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

package org.apache.ignite.internal.schema.marshaller.codegen;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.internal.schema.marshaller.Serializer;

/**
 * Generate {@link Serializer} method's bodies for simple types.
 */
class IdentityObjectMarshallerExprGenerator implements MarshallerCodeGenerator {
    /** Tuple column accessor. */
    private final TupleColumnAccessCodeGenerator columnAccessor;

    /**
     * Constructor.
     *
     * @param columnAccessor Tuple column code generator.
     */
    IdentityObjectMarshallerExprGenerator(TupleColumnAccessCodeGenerator columnAccessor) {
        this.columnAccessor = columnAccessor;
    }

    /** {@inheritDoc} */
    @Override public boolean isSimpleType() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public CodeBlock unmarshallObjectCode(String tupleExpr) {
        return CodeBlock.builder()
            .addStatement("return $L", columnAccessor.read(tupleExpr))
            .build();
    }

    /** {@inheritDoc} */
    @Override public CodeBlock marshallObjectCode(String asm, String objVar) {
        return columnAccessor.write(asm, objVar);
    }

    /** {@inheritDoc} */
    @Override public CodeBlock getValueCode(String objVar, int colIdx) {
        return CodeBlock.of(objVar);
    }

    /** {@inheritDoc} */
    @Override public void initStaticHandlers(TypeSpec.Builder builder, CodeBlock.Builder staticBuilder) {
        throw new UnsupportedOperationException("Static handlers are not applicable to simple types.");
    }
}
