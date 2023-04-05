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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.jul.NoOpHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.function.Executable;

class ItStartTest extends IgniteIntegrationTest {
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
                new Expectation("joinComplete", IgniteImpl.class, "Join complete, starting the remaining components"),
                new Expectation("indexManager", IndexManager.class, "Index manager started"),
                new Expectation("componentsStarted", IgniteImpl.class, "Components started, performing recovery"),
                new Expectation("recoveryComplete", IgniteImpl.class, "Recovery complete, finishing join"),
                new Expectation("startComplete", "org.apache.ignite.internal.app.LifecycleManager", "Start complete")
        );

        List<LoggingProbe> probes = expectations.stream()
                .map(ItStartTest::installProbe)
                .collect(toList());

        try {
            cluster.startAndInit(1);
        } finally {
            probes.forEach(LoggingProbe::cleanup);
        }

        assertAll(
                probes.stream()
                        .<Executable>map(probe -> () -> assertThat(
                                "Wrong thread for " + probe.expectation.name,
                                probe.threadNameRef.get(),
                                startsWith(startThreadNamePrefix())
                        ))
                        .collect(toList())
        );
    }

    private String startThreadNamePrefix() {
        return "%" + IgniteTestUtils.testNodeName(testInfo, 0) + "%start-";
    }

    private static LoggingProbe installProbe(Expectation expectation) {
        Logger logger = Logger.getLogger(expectation.loggerClassName);

        AtomicReference<String> threadNameRef = new AtomicReference<>();

        var handler = new NoOpHandler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage().matches(expectation.messageRegexp)) {
                    threadNameRef.set(Thread.currentThread().getName());
                }
            }
        };

        logger.addHandler(handler);

        return new LoggingProbe(expectation, logger, handler, threadNameRef);
    }

    @Test
    void startFutureCompletesInCommonPool() {
        cluster.startAndInit(1);

        AtomicReference<String> threadNameRef = new AtomicReference<>();

        CompletableFuture<IgniteImpl> future = cluster.startClusterNode(1).whenComplete((res, ex) -> {
            threadNameRef.set(Thread.currentThread().getName());
        });

        assertThat(future, willCompleteSuccessfully());

        assertThat(threadNameRef.get(), startsWith("ForkJoinPool.commonPool-"));

        cluster.shutdown();

        assertThatStartThreadsAreStopped();
    }

    private void assertThatStartThreadsAreStopped() {
        List<String> aliveStartThreads = Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(name -> name.startsWith(startThreadNamePrefix()))
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
        private final Logger logger;
        private final Handler handler;
        private final AtomicReference<String> threadNameRef;

        private LoggingProbe(Expectation expectation, Logger logger, Handler handler, AtomicReference<String> threadNameRef) {
            this.expectation = expectation;
            this.logger = logger;
            this.handler = handler;
            this.threadNameRef = threadNameRef;
        }

        void cleanup() {
            logger.removeHandler(handler);
        }
    }
}
