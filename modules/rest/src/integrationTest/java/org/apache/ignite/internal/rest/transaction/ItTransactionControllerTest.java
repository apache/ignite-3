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

import static io.micronaut.http.HttpStatus.NOT_FOUND;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

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
import org.apache.ignite.tx.Transaction;
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

    @Test
    void shouldReturnAllTransactions() {
        Transaction tx = node(0).transactions().begin();

        sql(tx, "SELECT 1");

        // Check count of transactions
        await().untilAsserted(() -> {
            Map<UUID, TransactionInfo> transactions = getTransactions(client);

            assertThat(transactions, aMapWithSize(greaterThan(1)));
            TransactionInfo transactionInfo = transactions.entrySet().iterator().next().getValue();

            assertThat(transactionInfo.type(), is("READ_WRITE"));
            assertThat(transactionInfo.state(), is("PENDING"));
            assertThat(transactionInfo.priority(), is("NORMAL"));
        });
    }

    @Test
    void shouldReturnTransactionById() {
        Transaction tx = node(0).transactions().begin();

        sql(tx, "SELECT 1");

        // Check count of transactions
        await().untilAsserted(() -> {
            Map<UUID, TransactionInfo> transactions = getTransactions(client);

            assertThat(transactions, aMapWithSize(greaterThan(1)));
            TransactionInfo transactionInfo = transactions.entrySet().iterator().next().getValue();

            TransactionInfo transaction = getTransaction(client, transactionInfo.id());
            assertThat(transaction.id(), is(transactionInfo.id()));
            assertThat(transaction.type(), is("READ_WRITE"));
            assertThat(transaction.state(), is("PENDING"));
            assertThat(transaction.priority(), is("NORMAL"));
        });
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

    private static Map<UUID, TransactionInfo> getTransactions(HttpClient client) {
        List<TransactionInfo> transactionInfos = client.toBlocking()
                .retrieve(HttpRequest.GET(""), Argument.listOf(TransactionInfo.class));

        return transactionInfos.stream().collect(Collectors.toMap(TransactionInfo::id, t -> t));
    }

    private static TransactionInfo getTransaction(HttpClient client, UUID transactionId) {
        return client.toBlocking().retrieve(HttpRequest.GET("/" + transactionId), TransactionInfo.class);
    }
}
