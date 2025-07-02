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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests work directory injection as a non-static field in a parameterized test class. In such a case forcing per class initialization is
 * needed so that it doesn't initialized in the BeforeEachCallback.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ParameterizedClass
@TestInstance(Lifecycle.PER_CLASS)
@ValueSource(ints = {0, 1})
class ParameterizedClassNonStaticWorkDirTest {
    @WorkDirectory(forcePerClassTemplate = true)
    private Path workDir;

    private final Path[] savedWorkDirs = new Path[2];

    @SuppressWarnings("unused")
    @Parameter
    private int index;

    @BeforeAll
    void beforeAll() {
        assertThat(workDir, is(nullValue()));
    }

    @BeforeParameterizedClassInvocation
    void beforeClassTemplate() {
        assertThat(workDir, is(notNullValue()));
        savedWorkDirs[index] = workDir;
    }

    @AfterAll
    void afterAll() {
        assertThat(savedWorkDirs[0], is(not(savedWorkDirs[1])));
    }

    @Test
    void test() {
        // Verify that the workDir was not overwritten by BeforeEachCallback
        assertThat(workDir, is(savedWorkDirs[index]));
    }
}
