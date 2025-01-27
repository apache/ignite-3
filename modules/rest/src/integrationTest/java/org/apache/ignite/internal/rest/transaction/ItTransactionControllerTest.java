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

package org.apache.ignite.internal.rest.transaction;

import static io.micronaut.http.HttpRequest.DELETE;
import static io.micronaut.http.HttpStatus.NOT_FOUND;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.rest.api.transaction.TransactionInfo;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link TransactionController}.
 */
@MicronautTest
public class ItTransactionControllerTest extends ClusterPerClassIntegrationTest {
    private static final String TRANSACTIONURL = "/management/v1/transaction/";

    @Inject
    @Client("http://localhost:10300" + TRANSACTIONURL)
    HttpClient client;

    @BeforeAll
    void beforeAll() {
        await(((SystemViewManagerImpl) unwrapIgniteImpl(CLUSTER.aliveNode()).systemViewManager()).completeRegistration());
    }

    @Test
    void shouldReturnAllTransactions() {
        Transaction roTx = node(0).transactions().begin(new TransactionOptions().readOnly(true));
        Transaction rwTx = node(0).transactions().begin(new TransactionOptions().readOnly(false));

        Map<UUID, TransactionInfo> transactions = getTransactions(client);

        {
            TransactionInfo transactionInfo = transactions.get(((InternalTransaction) roTx).id());

            assertThat(transactionInfo, notNullValue());
            assertThat(transactionInfo.type(), is("READ_ONLY"));
            assertThat(transactionInfo.state(), nullValue());
            assertThat(transactionInfo.priority(), is("NORMAL"));

            roTx.rollback();
        }

        {
            TransactionInfo transactionInfo = transactions.get(((InternalTransaction) rwTx).id());

            assertThat(transactionInfo, notNullValue());
            assertThat(transactionInfo.type(), is("READ_WRITE"));
            assertThat(transactionInfo.state(), is("PENDING"));
            assertThat(transactionInfo.priority(), is("NORMAL"));

            rwTx.rollback();
        }
    }

    @Test
    void shouldReturnTransactionById() {
        Transaction roTx = node(0).transactions().begin(new TransactionOptions().readOnly(true));
        Transaction rwTx = node(0).transactions().begin(new TransactionOptions().readOnly(false));

        TransactionInfo roTransactionInfo = getTransaction(client, ((InternalTransaction) roTx).id());
        {
            assertThat(roTransactionInfo, notNullValue());
            assertThat(roTransactionInfo.type(), is("READ_ONLY"));
            assertThat(roTransactionInfo.state(), nullValue());
            assertThat(roTransactionInfo.priority(), is("NORMAL"));

            roTx.rollback();
        }

        TransactionInfo rwTransactionInfo = getTransaction(client, ((InternalTransaction) rwTx).id());
        {
            assertThat(rwTransactionInfo, notNullValue());
            assertThat(rwTransactionInfo.type(), is("READ_WRITE"));
            assertThat(rwTransactionInfo.state(), is("PENDING"));
            assertThat(rwTransactionInfo.priority(), is("NORMAL"));

            rwTx.rollback();
        }
    }

    @Test
    void shouldReturnProblemIfRetrieveNonExistingTransaction() {
        UUID transactionId = UUID.randomUUID();

        assertThrowsProblem(
                () -> getTransaction(client, transactionId),
                NOT_FOUND,
                isProblem().withDetail("Transaction not found [transactionId=" + transactionId + "]")
        );
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24296")
    void shouldReturnProblemIfCancelNonExistingTransaction() {
        UUID transactionId = UUID.randomUUID();

        assertThrowsProblem(
                () -> cancelTransaction(client, transactionId),
                NOT_FOUND,
                isProblem().withDetail("Transaction not found [transactionId=" + transactionId + "]")
        );
    }

    private static Map<UUID, TransactionInfo> getTransactions(HttpClient client) {
        List<TransactionInfo> transactionInfos = client.toBlocking()
                .retrieve(HttpRequest.GET(""), Argument.listOf(TransactionInfo.class));

        return transactionInfos.stream().collect(Collectors.toMap(TransactionInfo::id, t -> t));
    }

    private static TransactionInfo getTransaction(HttpClient client, UUID transactionId) {
        return client.toBlocking().retrieve(HttpRequest.GET("/" + transactionId), TransactionInfo.class);
    }

    private static void cancelTransaction(HttpClient client, UUID transactionId) {
        client.toBlocking().exchange(DELETE("/" + transactionId));
    }
}
