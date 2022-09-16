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

package com.facebook.presto.bytecode.expression;

import java.util.List;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.instruction.Constant;

import static com.facebook.presto.bytecode.ParameterizedType.type;

class ConstantBytecodeExpression
    extends BytecodeExpression {
    private final Constant value;

    ConstantBytecodeExpression(Class<?> type, Constant value) {
        this(type(type), value);
    }

    ConstantBytecodeExpression(ParameterizedType type, Constant value) {
        super(type);
        this.value = value;
    }

    @Override
    public Constant getBytecode(MethodGenerationContext generationContext) {
        return value;
    }

    @Override
    protected String formatOneLine() {
        return renderConstant(value.getValue());
    }

    public static String renderConstant(Object value) {
        if (value instanceof Long) {
            return value + "L";
        }
        if (value instanceof Float) {
            return value + "f";
        }
        if (value instanceof ParameterizedType) {
            return ((ParameterizedType)value).getSimpleName() + ".class";
        }
        // escape string
        if (value instanceof String) {
            return "\"" + value + "\"";
        }
        return String.valueOf(value);
    }

    @Override
    public List<BytecodeNode> getChildNodes() {
        return List.of();
    }
}
