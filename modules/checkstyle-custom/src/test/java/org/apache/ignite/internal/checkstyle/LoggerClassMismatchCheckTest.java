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

package org.apache.ignite.internal.checkstyle;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.puppycrawl.tools.checkstyle.Checker;
import com.puppycrawl.tools.checkstyle.DefaultConfiguration;
import com.puppycrawl.tools.checkstyle.TreeWalker;
import com.puppycrawl.tools.checkstyle.api.AuditEvent;
import com.puppycrawl.tools.checkstyle.api.AuditListener;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link LoggerClassMismatchCheck}. */
class LoggerClassMismatchCheckTest {

    private static final String RESOURCE_DIR = "org/apache/ignite/internal/checkstyle/";

    @Test
    void correctUsageProducesNoViolation() throws Exception {
        List<AuditEvent> violations = runCheck("InputLoggerCorrect.java", new DefaultConfiguration(
                LoggerClassMismatchCheck.class.getName()));

        assertThat(violations, is(empty()));
    }

    @Test
    void mismatchedClassProducesViolation() throws Exception {
        List<AuditEvent> violations = runCheck("InputLoggerMismatched.java", new DefaultConfiguration(
                LoggerClassMismatchCheck.class.getName()));

        assertThat(violations, hasSize(1));
        assertThat(violations.get(0).getLine(), is(24));
        assertTrue(violations.get(0).getMessage().contains("SomeOtherClass"));
        assertTrue(violations.get(0).getMessage().contains("InputLoggerMismatched"));
    }

    @Test
    void nonLoggerFieldProducesNoViolation() throws Exception {
        List<AuditEvent> violations = runCheck("InputLoggerNonLoggerField.java", new DefaultConfiguration(
                LoggerClassMismatchCheck.class.getName()));

        assertThat(violations, is(empty()));
    }

    @Test
    void innerClassLoggerMatchesInnerClass() throws Exception {
        List<AuditEvent> violations = runCheck("InputLoggerInnerClass.java", new DefaultConfiguration(
                LoggerClassMismatchCheck.class.getName()));

        assertThat(violations, is(empty()));
    }

    @Test
    void innerClassMismatchProducesViolation() throws Exception {
        List<AuditEvent> violations = runCheck("InputLoggerInnerClassMismatch.java", new DefaultConfiguration(
                LoggerClassMismatchCheck.class.getName()));

        assertThat(violations, hasSize(1));
        assertThat(violations.get(0).getLine(), is(27));
        assertTrue(violations.get(0).getMessage().contains("InputLoggerInnerClassMismatch"));
        assertTrue(violations.get(0).getMessage().contains("Inner"));
    }

    @Test
    void customFieldNamePattern() throws Exception {
        DefaultConfiguration checkConfig = new DefaultConfiguration(
                LoggerClassMismatchCheck.class.getName());
        checkConfig.addProperty("fieldNamePattern", "MY_LOG");

        List<AuditEvent> violations = runCheck("InputLoggerCustomFieldName.java", checkConfig);

        assertThat(violations, hasSize(1));
        assertThat(violations.get(0).getLine(), is(24));
        assertTrue(violations.get(0).getMessage().contains("SomeOtherClass"));
    }

    @Test
    void multipleFactoryPatterns() throws Exception {
        List<AuditEvent> violations = runCheck("InputLoggerMultiplePatterns.java", new DefaultConfiguration(
                LoggerClassMismatchCheck.class.getName()));

        assertThat(violations, hasSize(2));
        assertThat(violations.get(0).getLine(), is(25));
        assertThat(violations.get(1).getLine(), is(27));
    }

    private List<AuditEvent> runCheck(String inputFile, DefaultConfiguration checkConfig) throws Exception {
        DefaultConfiguration treeWalkerConfig = new DefaultConfiguration(TreeWalker.class.getName());
        treeWalkerConfig.addChild(checkConfig);

        DefaultConfiguration checkerConfig = new DefaultConfiguration("Checker");
        checkerConfig.addChild(treeWalkerConfig);

        Checker checker = new Checker();
        checker.setModuleClassLoader(Thread.currentThread().getContextClassLoader());
        checker.configure(checkerConfig);

        List<AuditEvent> violations = new ArrayList<>();
        checker.addListener(new AuditListener() {
            @Override
            public void auditStarted(AuditEvent event) {
            }

            @Override
            public void auditFinished(AuditEvent event) {
            }

            @Override
            public void fileStarted(AuditEvent event) {
            }

            @Override
            public void fileFinished(AuditEvent event) {
            }

            @Override
            public void addError(AuditEvent event) {
                violations.add(event);
            }

            @Override
            public void addException(AuditEvent event, Throwable throwable) {
                throw new RuntimeException("Checkstyle exception", throwable);
            }
        });

        URL resource = getClass().getClassLoader().getResource(RESOURCE_DIR + inputFile);
        assert resource != null : "Test resource not found: " + inputFile;

        File file = new File(resource.toURI());
        List<File> files = List.of(file);

        checker.process(files);
        checker.destroy();

        return violations;
    }
}
