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
package org.apache.ignite.internal.configuration.processor;

import com.google.testing.compile.Compilation;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for basic code generation scenarios.
 */
public class ITProcessorTest extends AbstractProcessorTest {
    /**
     * The simplest test for code generation.
     */
    @Test
    public void testPublicCodeGeneration() {
        final String packageName = "org.apache.ignite.internal.configuration.processor";

        final ClassName testConfigurationSchema = ClassName.get(packageName, "TestConfigurationSchema");

        final BatchCompilation batch = batchCompile(testConfigurationSchema);

        final Compilation status = batch.getCompilationStatus();

        assertNotEquals(Compilation.Status.FAILURE, status.status());

        assertEquals(3, batch.generated().size());

        final ConfigSet classSet = batch.getBySchema(testConfigurationSchema);

        assertTrue(classSet.allGenerated());
    }

    /**
     * Check the successful code generation of the internal configurations.
     */
    @Test
    void testInternalCodeGenerationSuccess() {
        String packageName = "org.apache.ignite.internal.configuration.processor.internal";

        ClassName[] internalConfigSchemas = {
            ClassName.get(packageName, "InternalTestRootConfigurationSchema"),
            ClassName.get(packageName, "InternalTestConfigurationSchema"),
            // To test the extension.
            ClassName.get("org.apache.ignite.internal.configuration.processor", "TestConfigurationSchema"),
            ClassName.get(packageName, "ExtendedInternalTestConfigurationSchema"),
        };

        BatchCompilation batch = batchCompile(internalConfigSchemas);

        Compilation status = batch.getCompilationStatus();

        assertNotEquals(Compilation.Status.FAILURE, status.status());

        assertEquals(4 * 3, batch.generated().size());

        assertTrue(batch.getBySchema(internalConfigSchemas[0]).allGenerated());
        assertTrue(batch.getBySchema(internalConfigSchemas[1]).allGenerated());
        assertTrue(batch.getBySchema(internalConfigSchemas[2]).allGenerated());
        assertTrue(batch.getBySchema(internalConfigSchemas[3]).allGenerated());
    }

    /**
     * Check for errors in the generation of the internal root configuration code.
     */
    @Test
    void testInternalCodeGenerationErrorForRootConfiguration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.internal";

        assertThrows(
            Throwable.class,
            () -> batchCompile(ClassName.get(packageName, "ErrorInternalTestRootConfigurationSchema"))
        );
    }

    /**
     * Check for errors in the generation of the internal configuration code.
     */
    @Test
    void testInternalCodeGenerationErrorForConfiguration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.internal";

        assertThrows(
            Throwable.class,
            () -> batchCompile(ClassName.get(packageName, "ErrorInternalTestConfigurationSchema"))
        );
    }

    /**
     * Check for errors in the generation of the internal extended configuration code.
     */
    @Test
    void testInternalCodeGenerationErrorForExtendedConfiguration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.internal";

        assertThrows(
            Throwable.class,
            () -> batchCompile(ClassName.get(packageName, "ErrorExtendedInternalTestConfigurationSchema"))
        );
    }
}
