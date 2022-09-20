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
import org.junit.jupiter.api.Test;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressionAssertions.assertBytecodeExpression;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantDouble;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;

public class TestInvokeBytecodeExpression {
    @Test
    public void testInvokeMethod()
        throws Exception {
        assertBytecodeExpression(constantString("foo").invoke("length", int.class), "foo".length(), "\"foo\".length()");
        assertBytecodeExpression(constantString("foo").invoke("concat", String.class, constantString("bar")), "foo".concat("bar"), "\"foo\".concat(\"bar\")");
        assertBytecodeExpression(
            constantString("foo").invoke("concat", String.class, List.of(String.class), constantString("bar")),
            "foo".concat("bar"),
            "\"foo\".concat(\"bar\")");
        assertBytecodeExpression(
            constantString("foo").invoke("concat", type(String.class), List.of(type(String.class)), constantString("bar")),
            "foo".concat("bar"),
            "\"foo\".concat(\"bar\")");
    }

    @Test
    public void testInvokeStaticMethod()
        throws Exception {
        assertBytecodeExpression(invokeStatic(System.class, "lineSeparator", String.class), System.lineSeparator(), "System.lineSeparator()");
        assertBytecodeExpression(invokeStatic(Math.class, "cos", double.class, constantDouble(33.3)), Math.cos(33.3), "Math.cos(33.3)");
        assertBytecodeExpression(
            invokeStatic(Math.class, "cos", double.class, List.of(double.class), constantDouble(33.3)),
            Math.cos(33.3),
            "Math.cos(33.3)");
        assertBytecodeExpression(
            invokeStatic(type(Math.class), "cos", type(double.class), List.of(type(double.class)), List.of(constantDouble(33.3))),
            Math.cos(33.3),
            "Math.cos(33.3)");
    }
}
