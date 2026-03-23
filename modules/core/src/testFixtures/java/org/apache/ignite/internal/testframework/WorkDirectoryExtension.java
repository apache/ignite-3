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

package org.apache.ignite.internal.testframework;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.extension.ExtensionContext.Namespace;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.OperatingSystem;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;

/**
 * JUnit extension for injecting temporary folders into test classes.
 *
 * <p>This extension supports both field and parameter injection of {@link Path} parameters annotated with the {@link WorkDirectory}
 * annotation.
 *
 * <p>A new temporary folder can be created for every test method (when used as a test parameter or as a member field) or a single time in
 * a
 * test class' lifetime (when used as a parameter in a {@link BeforeAll} hook or as a static field). Temporary folders are located relative
 * to the module, where the tests are being run, and their paths depends on the lifecycle of the folder:
 *
 * <ol>
 *     <li>For test methods: "build/work/{@literal <name-of-the-test-class>/<name-of-the-test-method>_<current_time_millis>}"</li>
 *     <li>For test classes: "build/work/{@literal <name-of-the-test-class>/static_<current_time_millis>}"</li>
 * </ol>
 *
 * <p>Temporary folders are removed after tests have finished running, but this behaviour can be controlled by setting the
 * {@link WorkDirectoryExtension#KEEP_WORK_DIR_PROPERTY} property. See {@link #KEEP_WORK_DIR_PROPERTY} and {@link #ARTIFACT_DIR_PROPERTY}
 * for more information.
 */
public class WorkDirectoryExtension
        implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback, BeforeClassTemplateInvocationCallback,
        AfterClassTemplateInvocationCallback, ParameterResolver {
    /** JUnit namespace for the extension. */
    private static final Namespace NAMESPACE = Namespace.create(WorkDirectoryExtension.class);

    /**
     * System property that can be used to provide a comma-separated list of test names whose work directories should be preserved after
     * test execution. Test name consists of test class name, a dot and a test method name. In case if work directory is injected into the
     * static field and is shared between different tests, only test class name should be put into the list.
     * <br>
     * Example: {@code "FooBarTest.test1,FooBarTest.test2,StaticWorkDirectoryFieldTest,FooBarTest.test3"}.
     * Default value is {@code null}.
     */
    public static final String KEEP_WORK_DIR_PROPERTY = "KEEP_WORK_DIR";

    /**
     * System property that denotes the directory where archived work directory of the test, kept using {@link #KEEP_WORK_DIR_PROPERTY},
     * should be saved in. If defined, original work directory will be deleted. Default value is {@code null}.
     */
    public static final String ARTIFACT_DIR_PROPERTY = "ARTIFACT_DIR";

    /** Base path for all temporary folders in a module. */
    private static final Path BASE_PATH = getBasePath();

    /** Name of the work directory that will be injected into {@link BeforeAll} methods or static members. */
    private static final String STATIC_FOLDER_NAME = "static";

    /** Pattern for the {@link #KEEP_WORK_DIR_PROPERTY}. */
    private static final Pattern PATTERN = Pattern.compile("\\b\\w+(?:\\.\\w+)?(?:,\\b\\w+(?:\\.\\w+)?)*\\b");

    /**
     * Creates and injects a temporary directory into a static field.
     */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        Field workDirField = getWorkDirField(context);

        if (workDirField == null || !Modifier.isStatic(workDirField.getModifiers()) || isForcePerClassTemplate(workDirField)) {
            return;
        }

        workDirField.setAccessible(true);

        workDirField.set(null, createWorkDir(context));
    }

    /** {@inheritDoc} */
    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        cleanupWorkDir(context);

        Path testClassDir = getTestClassDir(context);

        // remove the folder for the current test class, if empty
        if (isEmpty(testClassDir)) {
            IgniteUtils.deleteIfExists(testClassDir);
        }

        // remove the base folder, if empty
        if (isEmpty(BASE_PATH)) {
            IgniteUtils.deleteIfExists(BASE_PATH);
        }
    }

    /**
     * Creates and injects a temporary directory into a field.
     */
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        Field workDirField = getWorkDirField(context);

        if (workDirField == null || Modifier.isStatic(workDirField.getModifiers()) || isForcePerClassTemplate(workDirField)) {
            return;
        }

        workDirField.setAccessible(true);

        workDirField.set(context.getRequiredTestInstance(), createWorkDir(context));
    }

    /** {@inheritDoc} */
    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        cleanupWorkDir(context);
    }

    @Override
    public void beforeClassTemplateInvocation(ExtensionContext context) throws Exception {
        Field workDirField = getWorkDirField(context);

        if (workDirField == null || !isForcePerClassTemplate(workDirField)) {
            return;
        }

        workDirField.setAccessible(true);

        workDirField.set(Modifier.isStatic(workDirField.getModifiers()) ? null : context.getRequiredTestInstance(), createWorkDir(context));
    }

    @Override
    public void afterClassTemplateInvocation(ExtensionContext context) throws Exception {
        cleanupWorkDir(context);
    }

    private static boolean isForcePerClassTemplate(Field field) {
        WorkDirectory workDirectory = field.getAnnotation(WorkDirectory.class);
        assert workDirectory != null;
        return workDirectory.forcePerClassTemplate();
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().equals(Path.class)
                && parameterContext.isAnnotated(WorkDirectory.class);
    }

    /** {@inheritDoc} */
    @Override
    public Object resolveParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        if (getWorkDirField(extensionContext) != null) {
            throw new IllegalStateException(
                    "Cannot perform parameter injection, because there exists a field annotated with @WorkDirectory"
            );
        }

        try {
            return createWorkDir(extensionContext);
        } catch (IOException e) {
            throw new ParameterResolutionException("Error when creating the work directory", e);
        }
    }

    /**
     * Creates a temporary folder for the given test method.
     */
    public static Path createWorkDir(ExtensionContext context) throws IOException {
        Path existingDir = context.getStore(NAMESPACE).get(context.getUniqueId(), Path.class);

        if (existingDir != null) {
            return existingDir;
        }

        Path workDir;

        Path testClassDir = getTestClassDir(context);

        String testMethodName = context.getTestMethod()
                .map(Method::getName)
                .orElse(STATIC_FOLDER_NAME);

        if (OperatingSystem.current() == OperatingSystem.WINDOWS) {
            testMethodName = IgniteTestUtils.shortTestMethodName(testMethodName);

            do {
                // Due to the fact that in the Windows operating system the path length limit is 260 characters.
                workDir = testClassDir.resolve(testMethodName + "_" + ThreadLocalRandom.current().nextInt(Short.MAX_VALUE));
            } while (Files.exists(workDir));
        } else {
            // Not using currentTimeMillis because some tests can have the same name (e.g. repeated tests) and execute in less than a
            // millisecond, which will result in identical paths being generated.
            workDir = testClassDir.resolve(testMethodName + '_' + System.nanoTime());
        }

        Files.createDirectories(workDir);

        context.getStore(NAMESPACE).put(context.getUniqueId(), workDir);

        return workDir;
    }

    /**
     * Returns a path to the working directory of the test class (identified by the JUnit context).
     */
    private static Path getTestClassDir(ExtensionContext context) {
        return BASE_PATH.resolve(context.getRequiredTestClass().getSimpleName());
    }

    /**
     * Removes a previously created work directory.
     */
    private static void cleanupWorkDir(ExtensionContext context) {
        Path workDir = context.getStore(NAMESPACE).remove(context.getUniqueId(), Path.class);

        String testClassName = context.getRequiredTestClass().getSimpleName();

        String testName = context.getTestMethod().map(method -> testClassName + "." + method.getName()).orElse(testClassName);

        if (workDir != null) {
            if (shouldKeepWorkDir(testName)) {
                String artifactDir = IgniteSystemProperties.getString(ARTIFACT_DIR_PROPERTY);

                if (artifactDir != null) {
                    Path artifactDirPath = Paths.get(artifactDir, testName + ".zip");

                    zipDirectory(workDir, artifactDirPath);

                    IgniteUtils.deleteIfExists(workDir);
                }
            } else {
                IgniteUtils.deleteIfExists(workDir);
            }
        }
    }

    private static boolean shouldKeepWorkDir(String testName) {
        String keepWorkDirStr = IgniteSystemProperties.getString(KEEP_WORK_DIR_PROPERTY);

        Set<String> keepWorkDirForTests;

        if (keepWorkDirStr != null) {
            if (!keepWorkDirPropertyValid(keepWorkDirStr)) {
                throw new IllegalArgumentException(KEEP_WORK_DIR_PROPERTY + " value " + keepWorkDirStr + " doesn't match pattern");
            }

            keepWorkDirForTests = Arrays.stream(keepWorkDirStr.split(",")).collect(toSet());
        } else {
            keepWorkDirForTests = Collections.emptySet();
        }

        return keepWorkDirForTests.contains(testName);
    }

    static boolean keepWorkDirPropertyValid(String property) {
        return PATTERN.matcher(property).matches();
    }

    /**
     * Creates a ZIP archive from the contents of a source directory.
     *
     * <p>This method recursively walks through the source directory and compresses all files (excluding directories)
     * into a ZIP archive at the target location. The directory structure is preserved within the ZIP file using
     * relative paths from the source directory.
     *
     * <p>The parent directories of the target ZIP file are created if they don't exist. If the target file already
     * exists, it will be overwritten.
     *
     * @param source the path to the source directory to be zipped; must be an existing directory
     * @param target the path where the ZIP archive will be created; parent directories will be created if necessary
     */
    public static void zipDirectory(Path source, Path target) {
        try {
            Files.createDirectories(target.getParent());

            Files.createFile(target);

            try (var zs = new ZipOutputStream(Files.newOutputStream(target))) {
                try (Stream<Path> filesStream = Files.walk(source)) {
                    filesStream.filter(path -> !Files.isDirectory(path))
                            .forEach(path -> {
                                var zipEntry = new ZipEntry(source.relativize(path).toString());
                                try {
                                    zs.putNextEntry(zipEntry);

                                    Files.copy(path, zs);

                                    zs.closeEntry();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Looks for the annotated field inside the given test class.
     *
     * @return Annotated field or {@code null} if no fields have been found
     * @throws IllegalStateException If more than one annotated fields have been found
     */
    @Nullable
    private static Field getWorkDirField(ExtensionContext context) {
        List<Field> fields = AnnotationSupport.findAnnotatedFields(
                context.getRequiredTestClass(),
                WorkDirectory.class,
                field -> field.getType().equals(Path.class),
                HierarchyTraversalMode.TOP_DOWN
        );

        if (fields.isEmpty()) {
            return null;
        }

        if (fields.size() != 1) {
            throw new IllegalStateException(String.format(
                    "Test class must have a single field of type 'java.nio.file.Path' annotated with '@WorkDirectory', "
                            + "but %d fields have been found",
                    fields.size()
            ));
        }

        return fields.get(0);
    }

    /**
     * Returns {@code true} if the given directory is empty or {@code false} if the given directory contains files or does not exist.
     */
    private static boolean isEmpty(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return false;
        }

        try (Stream<Path> list = Files.list(dir)) {
            return list.findAny().isEmpty();
        }
    }

    private static Path getBasePath() {
        return Path.of("build", "work");
    }
}
