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

package org.apache.ignite.internal.runner.app;

import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.placementdriver.negotiation.LeaseNegotiator;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class that checks that no excessive error messages are being printed on node stop.
 */
public class ItIgniteStopLogMessagesTest extends ClusterPerTestIntegrationTest {
    private static class FailureMessageInspector {
        private final LogInspector logInspector;

        private final String expectedMessage;

        private final AtomicInteger msgCount = new AtomicInteger();

        FailureMessageInspector(Class<?> loggerClass, String expectedMessage) {
            this.logInspector = LogInspector.create(loggerClass);
            this.expectedMessage = expectedMessage;

            logInspector.addHandler(
                    logEvent -> {
                        Throwable throwable = logEvent.getThrown();

                        return throwable != null
                                && unwrapCause(throwable) instanceof NodeStoppingException
                                && logEvent.getMessage().getFormattedMessage().contains(this.expectedMessage);
                    },
                    msgCount::incrementAndGet
            );
        }

        void start() {
            logInspector.start();
        }

        void stop() {
            logInspector.stop();
        }

        void assertNoMessages() {
            assertThat(String.format("Error message '%s' is present in the log", expectedMessage), msgCount.get(), is(0));
        }
    }

    private final List<FailureMessageInspector> logInspectors = List.of(
            new FailureMessageInspector(ReplicaManager.class, "Failed to stop replica"),
            new FailureMessageInspector(ReplicaManager.class, "Failed to process placement driver message"),
            new FailureMessageInspector(LeaseNegotiator.class, "Lease was not negotiated due to exception")
    );

    @BeforeEach
    void startLogInspectors() {
        logInspectors.forEach(FailureMessageInspector::start);
    }

    @AfterEach
    @Override
    public void tearDown() {
        super.tearDown();

        logInspectors.forEach(FailureMessageInspector::stop);

        logInspectors.forEach(FailureMessageInspector::assertNoMessages);
    }

    /**
     * Modifies the state of the cluster, actual assertions happen in the {@link #tearDown} method.
     */
    @Test
    void testNoErrorMessagesOnStop() {
        executeSql("CREATE TABLE TEST (key INT PRIMARY KEY)");
    }
}
