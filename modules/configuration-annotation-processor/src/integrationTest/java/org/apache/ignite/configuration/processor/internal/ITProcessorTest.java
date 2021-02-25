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
package org.apache.ignite.configuration.processor.internal;

import com.google.testing.compile.Compilation;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.configuration.processor.internal.HasFieldMatcher.hasFields;
import static org.apache.ignite.configuration.processor.internal.HasMethodMatcher.hasMethods;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for basic code generation scenarios.
 */
public class ITProcessorTest extends AbstractProcessorTest {
    /**
     * The simplest test for code generation.
     */
    @Test
    public void test() {
        final String packageName = "org.apache.ignite.configuration.processor.internal";

        final ClassName testConfigurationSchema = ClassName.get(packageName, "TestConfigurationSchema");

        final BatchCompilation batch = batchCompile(testConfigurationSchema);

        final Compilation status = batch.getCompilationStatus();

        assertNotEquals(Compilation.Status.FAILURE, status.status());

        assertEquals(6, batch.generated().size());

        final ConfigSet classSet = batch.getBySchema(testConfigurationSchema);

        assertTrue(classSet.allGenerated());

        assertThat(
            classSet.getNodeClass(),
            hasFields(
                "value1", Types.STRING,
                "primitiveLong", Types.LONG,
                "primitiveInt", Types.INT,
                "stringArray", Types.STRING_ARRAY
            )
        );

        assertThat(
            classSet.getNodeClass(),
            hasMethods(
                "value1()", Types.STRING,
                "primitiveLong()", "long",
                "primitiveInt()", "int",
                "stringArray()", Types.STRING_ARRAY,
                "initValue1(java.lang.String)", classSet.getNodeClass().getClassName(),
                "initPrimitiveLong(long)", classSet.getNodeClass().getClassName(),
                "initPrimitiveInt(int)", classSet.getNodeClass().getClassName(),
                "initStringArray(java.lang.String[])", classSet.getNodeClass().getClassName(),
                "changeValue1(java.lang.String)", classSet.getNodeClass().getClassName(),
                "changePrimitiveLong(long)", classSet.getNodeClass().getClassName(),
                "changePrimitiveInt(int)", classSet.getNodeClass().getClassName(),
                "changeStringArray(java.lang.String[])", classSet.getNodeClass().getClassName()
            )
        );
    }
}
