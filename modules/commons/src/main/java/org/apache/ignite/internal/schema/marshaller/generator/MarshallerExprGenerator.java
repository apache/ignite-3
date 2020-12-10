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

import org.apache.ignite.internal.schema.marshaller.Serializer;

/**
 * Generate {@link Serializer} method's bodies.
 */
class MarshallerExprGenerator {
    /** Object factory regerence expression. */
    private final String factoryRefExpr;

    /** Object field access expression generators. */
    protected FieldAccessExprGenerator[] accessors;

    /**
     * Constructor.
     *
     * @param factoryRefExpr Object factory regerence expression.
     * @param accessors Object field access expression generators.
     */
    public MarshallerExprGenerator(String factoryRefExpr, FieldAccessExprGenerator[] accessors) {
        this.accessors = accessors;
        this.factoryRefExpr = factoryRefExpr;
    }

    /**
     * @return {@code true} if it is simple object marshaller, {@code false} otherwise.
     */
    boolean isSimpleTypeMarshaller() {
        return factoryRefExpr == null;
    }

    /**
     * Appends unmashall object code to string builder.
     *
     * @param sb String builder.
     * @param indent Line indentation.
     */
    public void appendUnmarshallObjectExpr(StringBuilder sb, String indent) {
        assert factoryRefExpr != null;

        sb.append(indent).append("Object obj;" + JaninoSerializerGenerator.LF);
        // Try.
        sb.append(indent).append("try {" + JaninoSerializerGenerator.LF);
        sb.append(indent).append(JaninoSerializerGenerator.TAB + "obj = ").append(factoryRefExpr).append(".create();" + JaninoSerializerGenerator.LF);

        // Read column from tuple to object field.
        for (int i = 0; i < accessors.length; i++)
            accessors[i].appendPutFieldExpr(sb, accessors[i].readColumnExpr(), indent + JaninoSerializerGenerator.TAB);

        // Catch and rethrow wrapped exeption.
        sb.append(indent).append("} catch (Exception ex) {" + JaninoSerializerGenerator.LF);
        sb.append(indent).append(JaninoSerializerGenerator.TAB + "throw new SerializationException(\"Failed to instantiate object: \" + ")
            .append(factoryRefExpr).append(".getClazz().getName(), ex);").append(JaninoSerializerGenerator.LF);
        sb.append(indent).append("}" + JaninoSerializerGenerator.LF);
    }

    /**
     * Appends mashall object code to string builder.
     *
     * @param sb String builder.
     * @param indent Line indentation.
     */
    public void appendMarshallObjectExpr(StringBuilder sb, String indent) {
        // Try.
        sb.append(indent).append("try {" + JaninoSerializerGenerator.LF);

        // Write object field to tuple assembler.
        for (int i = 0; i < accessors.length; i++)
            accessors[i].appendWriteColumnExpr(sb, accessors[i].getFieldExpr(), indent + JaninoSerializerGenerator.TAB);

        // Catch and rethrow wrapped exeption.
        sb.append(indent).append("} catch (Exception ex) {" + JaninoSerializerGenerator.LF);
        sb.append(indent).append(JaninoSerializerGenerator.TAB + "throw new SerializationException(ex);").append(JaninoSerializerGenerator.LF);
        sb.append(indent).append("}" + JaninoSerializerGenerator.LF);
    }
}
