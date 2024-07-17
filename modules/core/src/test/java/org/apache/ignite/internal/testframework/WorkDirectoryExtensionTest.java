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

import static org.apache.ignite.internal.testframework.JunitExtensionTestUtils.assertExecutesSuccessfully;
import static org.apache.ignite.internal.testframework.JunitExtensionTestUtils.assertExecutesWithFailure;
import static org.apache.ignite.internal.testframework.WorkDirectoryExtension.keepWorkDirPropertyValid;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ParameterResolutionException;

/**
 * Tests for the {@link WorkDirectoryExtension}.
 *
 * <p>This class uses an approach when several nested classes are executed manually on the JUnit test engine, because some test methods
 * should fail as part of these meta-tests. Nested classes are skipped by the surefire plugin and must not be executed during the build.
 */
class WorkDirectoryExtensionTest {
    /**
     * Test class for the {@link #testStaticFieldInjection()} test.
     */
    @ExtendWith(WorkDirectoryExtension.class)
    static class NormalStaticFieldInjectionTest {
        @WorkDirectory
        private static Path workDir;

        private static Path testFile;

        @BeforeAll
        static void beforeAll() throws IOException {
            testFile = Files.createFile(workDir.resolve("foo"));
        }

        @RepeatedTest(3)
        public void test() {
            assertTrue(Files.exists(testFile));
        }
    }

    /**
     * Tests temporary folder injection into a static field by running a test multiple times and checking that the folder persists between
     * the runs.
     */
    @Test
    void testStaticFieldInjection() {
        assertExecutesSuccessfully(NormalStaticFieldInjectionTest.class);
    }

    /**
     * Test class for the {@link #testFieldInjection()} test.
     */
    @ExtendWith(WorkDirectoryExtension.class)
    static class NormalFieldInjectionTest {
        private static final Set<Path> paths = new HashSet<>();

        @WorkDirectory
        private Path workDir;

        @RepeatedTest(3)
        public void test() {
            assertThat(paths, not(contains(workDir)));

            for (Path path : paths) {
                assertTrue(Files.notExists(path));
            }

            paths.add(workDir);
        }
    }

    /**
     * Tests temporary folder injection into a field by running a test multiple times and checking that a new folder is created each time.
     */
    @Test
    void testFieldInjection() {
        assertExecutesSuccessfully(NormalFieldInjectionTest.class);
    }

    /**
     * Test class for the {@link #testMultipleMethodsInjection()} test.
     */
    @ExtendWith(WorkDirectoryExtension.class)
    static class MultipleMethodsInjectionTest {
        @BeforeEach
        void setUp(@WorkDirectory Path workDir) throws IOException {
            Files.createFile(workDir.resolve("foo"));
        }

        @Test
        void test(@WorkDirectory Path workDir) {
            assertTrue(Files.exists(workDir.resolve("foo")));
        }
    }

    /**
     * Tests a scenario when a folder is injected into both {@code BeforeEach} and test method and checks that it is the same folder, and it
     * does not get re-created.
     */
    @Test
    void testMultipleMethodsInjection() {
        assertExecutesSuccessfully(MultipleMethodsInjectionTest.class);
    }

    /**
     * Test class for the {@link #testDuplicateFieldAndParameterInjection()} test.
     */
    @ExtendWith(WorkDirectoryExtension.class)
    static class ErrorParameterResolutionTest {
        @WorkDirectory
        private static Path workDir;

        @BeforeEach
        void setUp(@WorkDirectory Path anotherWorkDir) {
            fail("Should not reach here");
        }

        @Test
        public void test() {
            fail("Should not reach here");
        }
    }

    /**
     * Tests an error condition when the {@code @WorkDirectory} annotation is placed on multiple elements.
     */
    @Test
    void testDuplicateFieldAndParameterInjection() {
        assertExecutesWithFailure(
                ErrorParameterResolutionTest.class,
                instanceOf(ParameterResolutionException.class),
                message(m -> m.contains("there exists a field annotated with @WorkDirectory"))
        );
    }

    /**
     * Test class for the {@link #testDuplicateFieldInjection()} test.
     */
    @ExtendWith(WorkDirectoryExtension.class)
    static class ErrorFieldInjectionTest {
        @WorkDirectory
        private static Path workDir1;

        @WorkDirectory
        private Path workDir2;

        @Test
        public void test() {
            fail("Should not reach here");
        }
    }

    /**
     * Tests an error condition when the {@code @WorkDirectory} annotation is placed on multiple fields.
     */
    @Test
    void testDuplicateFieldInjection() {
        assertExecutesWithFailure(
                ErrorFieldInjectionTest.class,
                instanceOf(IllegalStateException.class),
                message(m -> m.contains("Test class must have a single field of type"))
        );
    }

    /**
     * Test class for the {@link #testSystemProperty()} test.
     */
    @ExtendWith(SystemPropertiesExtension.class)
    @ExtendWith(WorkDirectoryExtension.class)
    static class SystemPropertiesTest {
        private static Path file1;

        private static Path file2;

        private static Path file3;

        private static final String TMP_PATH;

        static {
            try {
                TMP_PATH = Files.createTempDirectory("testdir").toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @AfterAll
        static void verify() {
            assertTrue(Files.exists(file1));
            assertFalse(Files.exists(file2));
            assertFalse(Files.exists(file3));
            assertTrue(Files.exists(Paths.get(TMP_PATH)));

            IgniteUtils.deleteIfExists(file1.getParent());
            IgniteUtils.deleteIfExists(Paths.get(TMP_PATH));

            System.clearProperty(WorkDirectoryExtension.ARTIFACT_DIR_PROPERTY);
        }

        @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
        @WithSystemProperty(key = WorkDirectoryExtension.KEEP_WORK_DIR_PROPERTY, value = "SystemPropertiesTest.test1")
        @Test
        void test1(@WorkDirectory Path workDir) throws IOException {
            file1 = Files.createFile(workDir.resolve("foo"));
        }

        @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
        @Test
        void test2(@WorkDirectory Path workDir) throws IOException {
            file2 = Files.createFile(workDir.resolve("foo"));
        }

        @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
        @WithSystemProperty(key = WorkDirectoryExtension.KEEP_WORK_DIR_PROPERTY, value = "SystemPropertiesTest.test3")
        @Test
        void test3(@WorkDirectory Path workDir) throws IOException {
            file3 = Files.createFile(workDir.resolve("foo"));

            System.setProperty(WorkDirectoryExtension.ARTIFACT_DIR_PROPERTY, TMP_PATH);
        }
    }

    /**
     * Test class for the {@link #testSystemPropertyWithStaticWorkDir()} test.
     */
    @ExtendWith(SystemPropertiesExtension.class)
    @ExtendWith(WorkDirectoryExtension.class)
    @WithSystemProperty(key = WorkDirectoryExtension.KEEP_WORK_DIR_PROPERTY, value = "SystemPropertiesTestWithStaticWorkDir")
    static class SystemPropertiesTestWithStaticWorkDir {
        private static Path file1;

        private static Path file2;

        @WorkDirectory
        static Path workDir;

        @AfterAll
        static void verify() {
            assertTrue(Files.exists(file1));
            assertTrue(Files.exists(file2));

            IgniteUtils.deleteIfExists(file1.getParent());
        }

        @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
        @Test
        void test1() throws IOException {
            file1 = Files.createFile(workDir.resolve("foo"));
        }

        @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
        @Test
        void test2() throws IOException {
            file2 = Files.createFile(workDir.resolve("bar"));
        }
    }

    /**
     * Test class for the {@link #testSystemPropertyWithMultipleTests()} test.
     */
    @ExtendWith(SystemPropertiesExtension.class)
    @ExtendWith(WorkDirectoryExtension.class)
    @WithSystemProperty(key = WorkDirectoryExtension.KEEP_WORK_DIR_PROPERTY, value = "SystemPropertiesTestWithMultipleTests.test1,"
            + "SystemPropertiesTestWithMultipleTests.test2")
    static class SystemPropertiesTestWithMultipleTests {
        private static Path file1;

        private static Path file2;

        @AfterAll
        static void verify() {
            assertTrue(Files.exists(file1));
            assertTrue(Files.exists(file2));

            IgniteUtils.deleteIfExists(file1.getParent());
        }

        @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
        @Test
        void test1(@WorkDirectory Path workDir) throws IOException {
            file1 = Files.createFile(workDir.resolve("foo"));
        }

        @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
        @Test
        void test2(@WorkDirectory Path workDir) throws IOException {
            file2 = Files.createFile(workDir.resolve("bar"));
        }
    }

    /**
     * Tests that a work directory can be preserved when a special system property is set.
     */
    @Test
    void testSystemProperty() {
        assertExecutesSuccessfully(SystemPropertiesTest.class);
    }

    /**
     * Tests that a static work directory can be preserved when a special system property is set.
     */
    @Test
    void testSystemPropertyWithStaticWorkDir() {
        assertExecutesSuccessfully(SystemPropertiesTestWithMultipleTests.class);
    }

    /**
     * Tests that a static work directory can be preserved when a special system property is set.
     */
    @Test
    void testSystemPropertyWithMultipleTests() {
        assertExecutesSuccessfully(SystemPropertiesTestWithMultipleTests.class);
    }

    /**
     * Tests {@link WorkDirectoryExtension#keepWorkDirPropertyValid}.
     */
    @Test
    void testKeepWorkDirectoryPattern() {
        assertTrue(keepWorkDirPropertyValid("Foo"));
        assertTrue(keepWorkDirPropertyValid("Foo.bar"));
        assertTrue(keepWorkDirPropertyValid("Foo,Foo"));
        assertTrue(keepWorkDirPropertyValid("Foo.bar,Foo"));
        assertTrue(keepWorkDirPropertyValid("Foo.bar,Foo.bar"));

        assertFalse(keepWorkDirPropertyValid("Foo#bar"));
        assertFalse(keepWorkDirPropertyValid("Foo.bar, Foo"));
        assertFalse(keepWorkDirPropertyValid("Foo ,Foo.bar"));
    }

    /**
     * Test class for the {@link #testEmptyClass()} test.
     */
    @ExtendWith(WorkDirectoryExtension.class)
    static class TestEmptyClass {
        @WorkDirectory
        private Path workDir;

        @Disabled("https://issues.apache.org/jira/browse/IGNITE-15799")
        @Test
        void test() {
        }
    }

    /**
     * Tests {@code WorkDirectoryExtension} lifecycle works correctly on a test class with all test methods being disabled.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-15799">IGNITE-15799</a>
     */
    @Test
    void testEmptyClass() {
        assertExecutesSuccessfully(TestEmptyClass.class);
    }
}
