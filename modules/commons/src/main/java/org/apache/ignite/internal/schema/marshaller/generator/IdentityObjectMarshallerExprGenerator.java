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
 * Generate {@link Serializer} method's bodies for simple types.
 */
class IdentityObjectMarshallerExprGenerator extends MarshallerExprGenerator {
    /**
     * Constructor.
     *
     * @param accessor Object field access expression generators.
     */
    IdentityObjectMarshallerExprGenerator(FieldAccessExprGenerator accessor) {
        super(null /* no instantiation needed */, new FieldAccessExprGenerator[] {accessor});
    }

    /** {@inheritDoc} */
    @Override public void appendMarshallObjectExpr(StringBuilder sb, String indent) {
        for (int i = 0; i < accessors.length; i++)
            accessors[i].appendWriteColumnExpr(sb, "obj", indent);
    }

    /** {@inheritDoc} */
    @Override public void appendUnmarshallObjectExpr(StringBuilder sb, String indent) {
        sb.append(indent).append("Object obj = ").append(accessors[0].readColumnExpr()).append(";" + JaninoSerializerGenerator.LF);
    }
}
