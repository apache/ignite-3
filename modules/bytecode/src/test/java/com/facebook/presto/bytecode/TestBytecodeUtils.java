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

package com.facebook.presto.bytecode;

import static com.facebook.presto.bytecode.BytecodeUtils.toJavaIdentifierString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestBytecodeUtils {
    @Test
    public void testToJavaIdentifierString() {
        assertEquals(toJavaIdentifierString("HelloWorld"), "HelloWorld");
        assertEquals(toJavaIdentifierString("Hello$World"), "Hello$World");
        assertEquals(toJavaIdentifierString("Hello#World"), "Hello_World");
        assertEquals(toJavaIdentifierString("A^B^C"), "A_B_C");
    }
}
