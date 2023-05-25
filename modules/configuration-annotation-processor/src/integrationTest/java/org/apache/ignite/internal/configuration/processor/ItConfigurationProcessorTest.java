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

package org.apache.ignite.internal.configuration.processor;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.getChangeName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.getConfigurationInterfaceName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.getViewName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.truth.StringSubject;
import com.google.testing.compile.Compilation;
import com.squareup.javapoet.ClassName;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.Name;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.opentest4j.AssertionFailedError;

/**
 * Test for basic code generation scenarios.
 */
public class ItConfigurationProcessorTest extends AbstractProcessorTest {
    /**
     * The simplest test for code generation.
     */
    @Test
    public void testPublicConfigCodeGeneration() {
        final String packageName = "org.apache.ignite.internal.configuration.processor";

        ClassName testConfigurationSchema = ClassName.get(packageName, "TestConfigurationSchema");

        BatchCompilation batch = batchCompile(testConfigurationSchema);

        Compilation status = batch.getCompilationStatus();

        assertEquals(Compilation.Status.SUCCESS, status.status());

        assertEquals(3, batch.generated().size());

        ConfigSet classSet = batch.getBySchema(testConfigurationSchema);

        assertTrue(classSet.allGenerated());
    }

    @Test
    void testSuccessInternalConfigCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.internal";

        ClassName cls0 = ClassName.get(packageName, "SimpleRootConfigurationSchema");
        ClassName cls1 = ClassName.get(packageName, "SimpleConfigurationSchema");
        ClassName cls2 = ClassName.get(packageName, "ExtendedSimpleRootConfigurationSchema");
        ClassName cls3 = ClassName.get(packageName, "ExtendedSimpleConfigurationSchema");

        BatchCompilation batchCompile = batchCompile(cls0, cls1, cls2, cls3);

        assertEquals(Compilation.Status.SUCCESS, batchCompile.getCompilationStatus().status());

        assertEquals(4 * 3, batchCompile.generated().size());

        assertTrue(batchCompile.getBySchema(cls0).allGenerated());
        assertTrue(batchCompile.getBySchema(cls1).allGenerated());
        assertTrue(batchCompile.getBySchema(cls2).allGenerated());
        assertTrue(batchCompile.getBySchema(cls3).allGenerated());
    }

    @Test
    void testErrorInternalConfigCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.internal";

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorInternal0ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorInternal1ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorInternal2ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(
                        packageName,
                        "SimpleRootConfigurationSchema",
                        "SimpleConfigurationSchema",
                        "ExtendedSimpleRootConfigurationSchema",
                        "ErrorInternal3ConfigurationSchema"
                )
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorInternal4ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(
                        packageName,
                        "SimpleRootConfigurationSchema",
                        "SimpleConfigurationSchema",
                        "ErrorInternal5ConfigurationSchema"
                )
        );
    }

    @Test
    void testErrorPolymorphicConfigCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.polymorphic";

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphic0ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphic1ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphic2ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphic3ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphic4ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphic5ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphic6ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphic7ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphic8ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphicInstance0ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphicInstance1ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphicInstance2ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphicInstance3ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphicInstance4ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphicInstance5ConfigurationSchema")
        );

        assertThrows(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorPolymorphicInstance6ConfigurationSchema")
        );
    }

    @Test
    void testSuccessPolymorphicConfigCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.polymorphic";

        ClassName cls0 = ClassName.get(packageName, "SimplePolymorphicConfigurationSchema");
        ClassName cls1 = ClassName.get(packageName, "SimplePolymorphicInstanceConfigurationSchema");
        ClassName cls2 = ClassName.get(packageName, "SimpleConfigurationSchema");
        ClassName cls3 = ClassName.get(packageName, "SimpleRootConfigurationSchema");

        BatchCompilation batchCompile = batchCompile(cls0, cls1, cls2, cls3);

        assertEquals(Compilation.Status.SUCCESS, batchCompile.getCompilationStatus().status());

        assertEquals(4 * 3, batchCompile.generated().size());

        assertTrue(batchCompile.getBySchema(cls0).allGenerated());
        assertTrue(batchCompile.getBySchema(cls1).allGenerated());
        assertTrue(batchCompile.getBySchema(cls2).allGenerated());
        assertTrue(batchCompile.getBySchema(cls3).allGenerated());
    }

    /**
     * Tests compilation of the static constants in schema classes. The test includes the following properties:
     * <ol>
     *     <li>Static constants are compiled without errors, even those of primitive type;</li>
     *     <li>Static constants do not get compiled into methods.</li>
     * </ol>
     */
    @Test
    void testStaticConstants() {
        String packageName = "org.apache.ignite.internal.configuration.processor.internal";

        ClassName schema = ClassName.get(packageName, "StaticConstantsConfigurationSchema");

        Compilation compilation = compile(schema);

        assertThat(compilation).succeededWithoutWarnings();

        var generatedClasses = List.of(
                getViewName(schema).toString(),
                getChangeName(schema).toString(),
                getConfigurationInterfaceName(schema).toString()
        );

        for (String generatedClass : generatedClasses) {
            StringSubject generatedContent = assertThat(compilation)
                    .generatedSourceFile(generatedClass)
                    .contentsAsUtf8String();

            generatedContent.doesNotContain("INT_CONSTANT");
            generatedContent.doesNotContain("STRING_CONSTANT");
        }
    }

    @Test
    void wrongSchemaPostfix() {
        String packageName = "org.apache.ignite.internal.configuration.processor";

        ClassName schema = ClassName.get(packageName, "ConfigurationSchemaWithWrongPostfix");

        Compilation compilation = compile(schema);

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining(schema + " must end with 'ConfigurationSchema'");
    }

    /**
     * Checks that compilation will fail due to misuse of {@link InjectedName}.
     */
    @Test
    void testErrorInjectedNameFieldCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.injectedname";

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorInjectedName0ConfigurationSchema"),
                "@InjectedName org.apache.ignite.internal.configuration.processor.injectedname.ErrorInjectedName0ConfigurationSchema.name"
                        + " field must be a String"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorInjectedName1ConfigurationSchema"),
                "org.apache.ignite.internal.configuration.processor.injectedname.ErrorInjectedName1ConfigurationSchema contains more than"
                        + " one field with @InjectedName"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorInjectedName2ConfigurationSchema"),
                "org.apache.ignite.internal.configuration.processor.injectedname.ErrorInjectedName2ConfigurationSchema.name must contain"
                        + " only one @InjectedName"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorInjectedName3ConfigurationSchema"),
                "@InjectedName org.apache.ignite.internal.configuration.processor.injectedname.ErrorInjectedName3ConfigurationSchema.name"
                        + " can only be present in a class annotated with @Config or @PolymorphicConfig"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorInjectedName4ConfigurationSchema"),
                "@InjectedName org.apache.ignite.internal.configuration.processor.injectedname.ErrorInjectedName4ConfigurationSchema.name2"
                        + " can only be present in a class annotated with @Config or @PolymorphicConfig"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorInjectedName5ConfigurationSchema"),
                "@InjectedName org.apache.ignite.internal.configuration.processor.injectedname.ErrorInjectedName5ConfigurationSchema.name2"
                        + " can only be present in a class annotated with @Config or @PolymorphicConfig"
        );
    }

    /**
     * Checks that compilation will fail due to misuse of {@link Name}.
     */
    @Test
    void testErrorNameFieldCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.injectedname";

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorName0ConfigurationSchema"),
                "@Name annotation can only be used with @ConfigValue: "
                        + "org.apache.ignite.internal.configuration.processor.injectedname.ErrorName0ConfigurationSchema.simple"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorName1ConfigurationSchema"),
                "Missing @Name for field: "
                        + "org.apache.ignite.internal.configuration.processor.injectedname.ErrorName1ConfigurationSchema.simple"
        );
    }

    /**
     * Checks that compilation will succeed when using {@link InjectedName}.
     */
    @Test
    void testSuccessInjectedNameFieldCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.injectedname";

        ClassName cls0 = ClassName.get(packageName, "SimpleConfigurationSchema");
        ClassName cls1 = ClassName.get(packageName, "PolyConfigurationSchema");

        BatchCompilation batchCompile = batchCompile(cls0, cls1);

        assertThat(batchCompile.getCompilationStatus()).succeededWithoutWarnings();

        assertEquals(2 * 3, batchCompile.generated().size());

        assertTrue(batchCompile.getBySchema(cls0).allGenerated());
        assertTrue(batchCompile.getBySchema(cls1).allGenerated());
    }

    /**
     * Checks that compilation will succeed when using {@link Name}.
     */
    @Test
    void testSuccessNameFieldCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.injectedname";

        ClassName cls0 = ClassName.get(packageName, "SimpleConfigurationSchema");
        ClassName cls1 = ClassName.get(packageName, "NameConfigurationSchema");

        BatchCompilation batchCompile = batchCompile(cls0, cls1);

        assertThat(batchCompile.getCompilationStatus()).succeededWithoutWarnings();

        assertEquals(2 * 3, batchCompile.generated().size());

        assertTrue(batchCompile.getBySchema(cls0).allGenerated());
        assertTrue(batchCompile.getBySchema(cls1).allGenerated());
    }

    /**
     * Checks that compilation will fail due to misuse of {@link InternalId}.
     */
    @Test
    void testErrorInternalIdFieldCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.internalid";

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ErrorInternalId0ConfigurationSchema"),
                "@InternalId org.apache.ignite.internal.configuration.processor.internalid.ErrorInternalId0ConfigurationSchema.id"
                        + " field must be a UUID"
        );
    }

    /**
     * Checks that compilation will succeed when using {@link InternalId}.
     */
    @Test
    void testSuccessInternalIdFieldCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.internalid";

        ClassName cls = ClassName.get(packageName, "SimpleInternalId0ConfigurationSchema");

        BatchCompilation batchCompile = batchCompile(cls);

        assertThat(batchCompile.getCompilationStatus()).succeededWithoutWarnings();

        assertEquals(3, batchCompile.generated().size());

        assertTrue(batchCompile.getBySchema(cls).allGenerated());
    }

    @Test
    void testFailValidationForAbstractConfiguration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.abstractconfig.validation";

        // Let's check the incompatible configuration schema annotations.

        Pattern incompatibleAnotationsPattern = Pattern.compile(
                ".*ConfigurationProcessorException: Class with @.* is not allowed with @.*"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "IncompatibleSchemaAnnotations0ConfigurationSchema"),
                incompatibleAnotationsPattern
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "IncompatibleSchemaAnnotations1ConfigurationSchema"),
                incompatibleAnotationsPattern
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "IncompatibleSchemaAnnotations2ConfigurationSchema"),
                incompatibleAnotationsPattern
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "IncompatibleSchemaAnnotations3ConfigurationSchema"),
                incompatibleAnotationsPattern
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "IncompatibleSchemaAnnotations4ConfigurationSchema"),
                incompatibleAnotationsPattern
        );

        // Let's check other validations for abstract configuration.

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "MustNotBeSuperClassConfigurationSchema"),
                "Class with @AbstractConfiguration should not have a superclass"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "MustNotContainPolymorphicIdConfigurationSchema"),
                "Class with @AbstractConfiguration cannot have a field with @PolymorphicId"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "FieldWithInjectedNameFromAbstractConfigMustContainsNameConfigurationSchema"),
                "Missing @Name for field"
        );

        // Let's check the validation of the abstract configuration descendants.

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ConfigRootCanExtendOnlyAbstractConfigurationConfigurationSchema"),
                "Superclass must have @AbstractConfiguration"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ConfigCanExtendOnlyAbstractConfigurationConfigurationSchema"),
                "Superclass must have @AbstractConfiguration"
        );

        // Let's check the search for a conflict of field names.

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ConfigRootConflictFieldNamesWithAbstractConfigConfigurationSchema"),
                "Duplicate field names are not allowed"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ConfigConflictFieldNamesWithAbstractConfigConfigurationSchema"),
                "Duplicate field names are not allowed"
        );

        // Let's check @InjectedName for descendants of abstract configuration.

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ConfigRootMustNotContainInjectedNameInAbstractConfigConfigurationSchema"),
                "Field with @InjectedName in superclass are not allowed"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ConfigMustNotContainInjectedNameWithAbstractConfigConfigurationSchema"),
                "Field with @InjectedName is already present in the superclass"
        );

        // Let's check @InternalId for descendants of abstract configuration.

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ConfigRootMustNotContainInternalIdInAbstractConfigConfigurationSchema"),
                "Field with @InternalId in superclass are not allowed"
        );

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "ConfigMustNotContainInternalIdWithAbstractConfigConfigurationSchema"),
                "Field with @InternalId is already present in the superclass"
        );

        // Let's check @Secret must be String.

        assertThrowsEx(
                IllegalStateException.class,
                () -> batchCompile(packageName, "SecretMustBeStringConfigurationSchema"),
                "must be String. Only String field can be annotated with @Secret"
        );
    }

    @Test
    void testSuccessfulCodeGenerationAbstractConfigurationAndItsDescendants() {
        String packageName = "org.apache.ignite.internal.configuration.processor.abstractconfig";

        ClassName abstractConfigSchema = ClassName.get(packageName, "AbstractConfigConfigurationSchema");
        ClassName abstractRootConfigSchema = ClassName.get(packageName, "AbstractRootConfigConfigurationSchema");

        ClassName configSchema = ClassName.get(packageName, "SimpleConfigConfigurationSchema");
        ClassName configRootSchema = ClassName.get(packageName, "SimpleConfigRootConfigurationSchema");

        BatchCompilation batchCompile = batchCompile(abstractConfigSchema, abstractRootConfigSchema, configSchema, configRootSchema);

        assertThat(batchCompile.getCompilationStatus()).succeededWithoutWarnings();

        assertEquals(4 * 3, batchCompile.generated().size());

        assertTrue(batchCompile.getBySchema(abstractConfigSchema).allGenerated());
        assertTrue(batchCompile.getBySchema(abstractRootConfigSchema).allGenerated());
        assertTrue(batchCompile.getBySchema(configSchema).allGenerated());
        assertTrue(batchCompile.getBySchema(configRootSchema).allGenerated());

        StringSubject configViewInterfaceContent = assertThat(batchCompile.getCompilationStatus())
                .generatedSourceFile(getViewName(configSchema).toString())
                .contentsAsUtf8String();

        StringSubject configRootViewInterfaceContent = assertThat(batchCompile.getCompilationStatus())
                .generatedSourceFile(getViewName(configRootSchema).toString())
                .contentsAsUtf8String();

        configViewInterfaceContent.contains("extends " + getViewName(abstractConfigSchema).simpleName());
        configRootViewInterfaceContent.contains("extends " + getViewName(abstractRootConfigSchema).simpleName());

        StringSubject configChangeInterfaceContent = assertThat(batchCompile.getCompilationStatus())
                .generatedSourceFile(getChangeName(configSchema).toString())
                .contentsAsUtf8String();

        StringSubject configRootChangeInterfaceContent = assertThat(batchCompile.getCompilationStatus())
                .generatedSourceFile(getChangeName(configRootSchema).toString())
                .contentsAsUtf8String();

        configChangeInterfaceContent.contains("extends " + getViewName(configSchema).simpleName()
                + ", " + getChangeName(abstractConfigSchema).simpleName());

        configRootChangeInterfaceContent.contains("extends " + getViewName(configRootSchema).simpleName()
                + ", " + getChangeName(abstractRootConfigSchema).simpleName());

        StringSubject configConfigurationInterfaceContent = assertThat(batchCompile.getCompilationStatus())
                .generatedSourceFile(getConfigurationInterfaceName(configSchema).toString())
                .contentsAsUtf8String();

        StringSubject configRootConfigurationInterfaceContent = assertThat(batchCompile.getCompilationStatus())
                .generatedSourceFile(getConfigurationInterfaceName(configRootSchema).toString())
                .contentsAsUtf8String();

        configConfigurationInterfaceContent.contains("extends " + getConfigurationInterfaceName(abstractConfigSchema).simpleName());
        configRootConfigurationInterfaceContent.contains("extends " + getConfigurationInterfaceName(abstractRootConfigSchema).simpleName());
    }

    /**
     * Compile set of classes.
     *
     * @param packageName Package names.
     * @param classNames Simple class names.
     * @return Result of batch compilation.
     */
    private static BatchCompilation batchCompile(String packageName, String... classNames) {
        ClassName[] classes = Arrays.stream(classNames)
                .map(clsName -> ClassName.get(packageName, clsName))
                .toArray(ClassName[]::new);

        return batchCompile(classes);
    }

    /**
     * Extends {@link Assertions#assertThrows(Class, Executable)} to check for a substring in the error message.
     *
     * @param expErrCls Expected error class.
     * @param exec Supplier.
     * @param expSubStr Expected substring in error message.
     * @throws AssertionFailedError If failed.
     */
    private void assertThrowsEx(Class<? extends Throwable> expErrCls, Executable exec, @Nullable String expSubStr) {
        Throwable t = assertThrows(expErrCls, exec);

        if (expSubStr != null) {
            assertThat(t.getMessage(), containsString(expSubStr));
        }
    }

    /**
     * Extends {@link Assertions#assertThrows(Class, Executable)} to validate an error message against a pattern.
     *
     * @param expErrCls Expected error class.
     * @param exec Supplier.
     * @param pattern Error message pattern.
     * @throws AssertionFailedError If failed.
     */
    private void assertThrowsEx(Class<? extends Throwable> expErrCls, Executable exec, Pattern pattern) {
        Throwable t = assertThrows(expErrCls, exec);

        assertThat(t.getMessage().replaceAll("[\\r\\n]", " "), matchesRegex(pattern));
    }
}
