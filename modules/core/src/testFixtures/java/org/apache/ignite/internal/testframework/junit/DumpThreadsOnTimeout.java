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

package org.apache.ignite.internal.testframework.junit;

import com.google.auto.service.AutoService;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.LifecycleMethodExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;

/**
 * This extension prints a thread dump (to the standard out) if a testable method (for instance, a method annotated with
 * {@link org.junit.jupiter.api.Test} or a lifecycle method (like {@link org.junit.jupiter.api.BeforeEach} method) times out.
 *
 * <p>This extension is designed to be
 * <a href="https://junit.org/junit5/docs/current/user-guide/#extensions-registration-automatic">automatically registered</a>
 * via META-INF/services/org.junit.jupiter.api.extension.Extension.
 * For this to work, system property {@code junit.jupiter.extensions.autodetection.enabled} must be set to {@code true}.
 * If the property is set (currently, this is done via Gradle build scripts), it is enough to add this module as a dependency
 * to make tests automatically register this extension, like this:
 *
 * <pre>
 * integrationTestImplementation testFixtures(project(':ignite-core'))
 * </pre>
 */
@AutoService(Extension.class)
public class DumpThreadsOnTimeout implements TestExecutionExceptionHandler, LifecycleMethodExecutionExceptionHandler {
    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        handleThrowable(throwable, "Test method");
    }

    private static void handleThrowable(Throwable throwable, String category) throws Throwable {
        if (isJunitMethodTimeout(throwable)) {
            // We take throwable.getMessage() because TimeoutExtension puts a description of 'what has timed out' to it.
            System.out.println(category + " " + throwable.getMessage() + ", dumping threads");

            dumpThreadsToStdout();
        }

        // Rethrow the thing to make sure the framework sees the failure.
        throw throwable;
    }

    /**
     * Returns {@code true} if provided exception is a TimeoutException thrown by JUnit, {@code false} otherwise.
     */
    public static boolean isJunitMethodTimeout(Throwable throwable) {
        // This is a pretty hacky and not too reliable way to determine whether this is the timeout event we are
        // interested in (meaning, a timed-out testable/lifecycle method execution), but it looks like this is the
        // best we can do.
        return throwable instanceof TimeoutException
                && throwable.getMessage() != null && throwable.getMessage().contains(" timed out after ");
    }

    private static void dumpThreadsToStdout() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] infos = bean.dumpAllThreads(true, true);

        for (ThreadInfo info : infos) {
            System.out.println(info);
        }
    }

    @Override
    public void handleBeforeAllMethodExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        handleThrowable(throwable, "Before all method");
    }

    @Override
    public void handleBeforeEachMethodExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        handleThrowable(throwable, "Before each method");
    }

    @Override
    public void handleAfterEachMethodExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        handleThrowable(throwable, "After each method");
    }

    @Override
    public void handleAfterAllMethodExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        handleThrowable(throwable, "After all method");
    }
}
