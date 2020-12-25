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

/**
 * Marshaller code generator.
 */
interface MarshallerCodeGenerator {
    /**
     * @return {@code true} if it is simple object marshaller, {@code false} otherwise.
     */
    boolean isSimpleType();

    /**
     * @param tupleExpr Tuple to read from.
     * @return Unmarshall object code.
     */
    CodeBlock unmarshallObjectCode(String tupleExpr);

    /**
     * @param asm Tuple assembler to write to.
     * @param objVar Object to serialize.
     * @return Marshall object code.
     */
    CodeBlock marshallObjectCode(String asm, String objVar);

    /**
     * @param objVar Object var.
     * @param colIdx Column index.
     * @return Object field value for given column.
     */
    CodeBlock getValueCode(String objVar, int colIdx);

    /**
     * @param classBuilder Class builder.
     * @param tClassExpr Target class expression.
     * @param staticInitBuilder Static initializer builder.
     */
    void initStaticHandlers(TypeSpec.Builder classBuilder, String tClassExpr, CodeBlock.Builder staticInitBuilder);
}
