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

package org.apache.ignite.migrationtools.cli.persistence.calls;

import static org.apache.ignite3.lang.ErrorGroups.Client.CONNECTION_ERR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.migrationtools.cli.persistence.params.IgniteClientAuthenticatorParams;
import org.apache.ignite.migrationtools.cli.persistence.params.MigrateCacheParams;
import org.apache.ignite.migrationtools.cli.persistence.params.PersistenceParams;
import org.apache.ignite.migrationtools.cli.persistence.params.RetrieableMigrateCacheParams;
import org.apache.ignite3.client.IgniteClientConnectionException;
import org.apache.ignite3.internal.cli.core.call.CallOutput;
import org.apache.ignite3.internal.cli.core.call.CallOutputStatus;
import org.apache.ignite3.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite3.internal.cli.core.exception.IgniteCliException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/** Tests for {@link RetriableMigrateCacheCall}. */
class RetriableMigrateCacheCallTest {
    private static final Path fakePath = Path.of("/");

    static List<DefaultCallOutput> retriableStatus() {
        return Collections.singletonList(
                DefaultCallOutput.<MigrateCacheCall.Ouput>builder()
                    .status(CallOutputStatus.ERROR)
                    .cause(new IgniteCliException("Error while migrating persistence folder"))
                    .body(new MigrateCacheCall.Ouput("Error while migrating persistence folder", fakePath))
                    .build()
        );
    }

    static List<DefaultCallOutput> unretriableStatus() {
        return List.of(
                DefaultCallOutput.success(new MigrateCacheCall.Ouput("Migration finished successfully", fakePath)),
                DefaultCallOutput.failure(new MigrateCacheCall.InvalidProgressFileException("Some error message")),
                DefaultCallOutput.failure(new IgniteCliException("Some random wrapped exception")),
                DefaultCallOutput.failure(new IgniteClientConnectionException(CONNECTION_ERR, "Client failed to connect", ""))
        );
    }

    static List<List<DefaultCallOutput>> execChain4() {
        List<List<DefaultCallOutput>> statuses = new ArrayList<>();
        for (var s0 : retriableStatus()) {
            for (var s1 : retriableStatus()) {
                for (var s2 : retriableStatus()) {
                    for (var s3 : unretriableStatus()) {
                        var status = List.of(s0, s1, s2, s3);
                        statuses.add(status);
                    }
                }
            }
        }
        return statuses;
    }

    static List<List<DefaultCallOutput>> execChain3() {
        List<List<DefaultCallOutput>> statuses = new ArrayList<>();
        for (var s0 : retriableStatus()) {
            for (var s1 : retriableStatus()) {
                for (var s2 : unretriableStatus()) {
                    var status = List.of(s0, s1, s2);
                    statuses.add(status);
                }
            }
        }
        return statuses;
    }

    static List<List<DefaultCallOutput>> execChain2() {
        List<List<DefaultCallOutput>> statuses = new ArrayList<>();
        for (var s0 : retriableStatus()) {
            for (var s1 : unretriableStatus()) {
                var status = List.of(s0, s1);
                statuses.add(status);
            }
        }
        return statuses;
    }

    static List<List<DefaultCallOutput>> execChain1() {
        List<List<DefaultCallOutput>> statuses = new ArrayList<>();
        for (var s0 : unretriableStatus()) {
            var status = List.of(s0);
            statuses.add(status);
        }
        return statuses;
    }

    @ParameterizedTest(name = "{index}: Status:{0}")
    @MethodSource({"retriableStatus", "unretriableStatus"})
    void checkCallWithoutRetries(DefaultCallOutput expectedStatus) {
        checkCallWithRetries(List.of(expectedStatus), 0, 1);
    }

    @ParameterizedTest(name = "{index}: RetryLimit:3, ExpectedExecution:1 Status:{0}")
    @MethodSource("execChain1")
    void checkLimit3Executions1(List<DefaultCallOutput> execStatus) {
        checkCallWithRetries(execStatus, 3, 1);
    }

    @ParameterizedTest(name = "{index}: RetryLimit:3, ExpectedExecution:2 Status:{0}")
    @MethodSource("execChain2")
    void checkLimit3Executions2(List<DefaultCallOutput> execStatus) {
        checkCallWithRetries(execStatus, 3, 2);
    }

    @ParameterizedTest(name = "{index}: RetryLimit:3, ExpectedExecution:3 Status:{0}")
    @MethodSource("execChain3")
    void checkLimit3Executions3(List<DefaultCallOutput> execStatus) {
        checkCallWithRetries(execStatus, 3, 3);
    }

    @ParameterizedTest(name = "{index}: RetryLimit:3, ExpectedExecution:4 Status:{0}")
    @MethodSource("execChain4")
    void checkWith3RetriesSuccess(List<DefaultCallOutput> execStatus) {
        checkCallWithRetries(execStatus, 3, 4);
    }

    void checkCallWithRetries(List<DefaultCallOutput> execStatus, int retryLimit, int numExpectedExecutions) {
        Iterator<DefaultCallOutput> it = execStatus.iterator();
        var migrateCacheCall = mock(MigrateCacheCall.class);
        Mockito.doAnswer(ans -> it.next())
                .when(migrateCacheCall)
                .execute(any(MigrateCacheCall.Input.class));

        var c = new RetriableMigrateCacheCall(migrateCacheCall);
        var retryParams = new RetrieableMigrateCacheParams(retryLimit, 0);
        CallOutput<MigrateCacheCall.Ouput> ret = c.execute(
                new RetriableMigrateCacheCall.Input(
                        mock(PersistenceParams.class),
                        mock(MigrateCacheParams.class),
                        retryParams,
                        mock(IgniteClientAuthenticatorParams.class))
        );

        Mockito.verify(migrateCacheCall, times(numExpectedExecutions)).execute(any(MigrateCacheCall.Input.class));

        var expectedStatus = execStatus.get(numExpectedExecutions - 1);
        assertThat(ret).isSameAs(expectedStatus);
    }
}
