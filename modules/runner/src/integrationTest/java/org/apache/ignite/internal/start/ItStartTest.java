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

package org.apache.ignite.internal.start;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.EmbeddedNode;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.internal.testframework.log4j2.LogInspector.Handler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

@ExtendWith(WorkDirectoryExtension.class)
class ItStartTest extends BaseIgniteAbstractTest {
    private Cluster cluster;

    @WorkDirectory
    private Path workDir;

    private TestInfo testInfo;

    @BeforeEach
    void createCluster(TestInfo testInfo) {
        cluster = new Cluster(testInfo, workDir);
    }

    @BeforeEach
    void storeTestInfo(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    void stopCluster() {
        cluster.shutdown();
    }

    @Test
    void igniteStartsInStartPool() {
        List<Expectation> expectations = List.of(
                new Expectation("joinComplete", IgniteImpl.class, "Join complete, starting MetaStorage"),
                new Expectation("msComplete", IgniteImpl.class, "MetaStorage started, starting the remaining components"),
                new Expectation("indexManager", IndexManager.class, "Index manager started"),
                new Expectation("componentsStarted", IgniteImpl.class, "Components started, performing recovery"),
                new Expectation("recoveryComplete", IgniteImpl.class, "Recovery complete, finishing join"),
                new Expectation("startComplete", "org.apache.ignite.internal.app.LifecycleManager", "Start complete")
        );

        Map<String, LogInspector> inspectors = new HashMap<>();

        List<LoggingProbe> probes = expectations.stream()
                .map(expectation -> installProbe(expectation, inspectors))
                .collect(toList());

        try {
            cluster.startAndInit(1);
        } finally {
            probes.forEach(LoggingProbe::cleanup);
            inspectors.values().forEach(LogInspector::stop);
        }

        assertAll(
                probes.stream()
                        .<Executable>map(probe -> () -> assertThat(
                                "Wrong thread for " + probe.expectation.name,
                                probe.threadNameRef.get(),
                                startsWith(joinThreadNamePrefix())
                        ))
                        .collect(toList())
        );
    }

    private String startThreadNamePrefix() {
        return "%" + IgniteTestUtils.testNodeName(testInfo, 0) + "%start-";
    }

    private String joinThreadNamePrefix() {
        return "%" + IgniteTestUtils.testNodeName(testInfo, 0) + "%join-";
    }

    private static LoggingProbe installProbe(Expectation expectation, Map<String, LogInspector> inspectors) {
        LogInspector inspector = inspectors.computeIfAbsent(
                expectation.loggerClassName,
                loggerClassName -> LogInspector.create(loggerClassName, true));

        AtomicReference<String> threadNameRef = new AtomicReference<>();

        Handler handler = inspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().matches(expectation.messageRegexp),
                () -> threadNameRef.set(Thread.currentThread().getName()));

        return new LoggingProbe(expectation, inspector, handler, threadNameRef);
    }

    @Test
    void startFutureCompletesInCommonPool() {
        cluster.startAndInit(1);

        AtomicReference<String> threadNameRef = new AtomicReference<>();

        EmbeddedNode node = cluster.startEmbeddedNode(1);
        CompletableFuture<Ignite> future = node.igniteAsync().whenComplete((res, ex) -> {
            threadNameRef.set(Thread.currentThread().getName());
        });

        assertThat(future, willCompleteSuccessfully());

        assertThat(threadNameRef.get(), startsWith("ForkJoinPool.commonPool-"));

        cluster.shutdown();

        assertThatThreadsAreStopped(startThreadNamePrefix());
        assertThatThreadsAreStopped(joinThreadNamePrefix());
    }

    private static void assertThatThreadsAreStopped(String threadNamePrefix) {
        List<String> aliveStartThreads = Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(name -> name.startsWith(threadNamePrefix))
                .collect(toList());

        assertThat(aliveStartThreads, is(empty()));
    }

    private static class Expectation {
        private final String name;
        private final String loggerClassName;
        private final String messageRegexp;

        private Expectation(String name, Class<?> loggerClass, String messageRegexp) {
            this(name, loggerClass.getName(), messageRegexp);
        }

        private Expectation(String name, String loggerClassName, String messageRegexp) {
            this.name = name;
            this.loggerClassName = loggerClassName;
            this.messageRegexp = messageRegexp;
        }
    }

    private static class LoggingProbe {
        private final Expectation expectation;
        private final LogInspector inspector;
        private final Handler handler;
        private final AtomicReference<String> threadNameRef;

        private LoggingProbe(Expectation expectation, LogInspector inspector, Handler handler, AtomicReference<String> threadNameRef) {
            this.expectation = expectation;
            this.inspector = inspector;
            this.handler = handler;
            this.threadNameRef = threadNameRef;
        }

        void cleanup() {
            inspector.removeHandler(handler);
        }
    }
}
