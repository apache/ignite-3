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

package org.apache.ignite.internal.sql.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Session.SessionBuilder;
import org.apache.ignite.tx.IgniteTransactions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests to verify {@link IgniteSqlImpl}.
 */
@SuppressWarnings({"ThrowableNotThrown", "resource"})
@ExtendWith(MockitoExtension.class)
class IgniteSqlImplTest extends BaseIgniteAbstractTest {
    @Test
    void allSessionsAreClosedAfterFacadeIsStopped() throws Exception {
        IgniteSqlImpl facade = newSqlFacade();

        SessionBuilder builder = facade.sessionBuilder();

        List<Session> sessions = new ArrayList<>();

        sessions.add(builder.build());
        sessions.add(builder.build());
        sessions.add(facade.sessionBuilder().build());
        sessions.add(facade.createSession());

        for (Session session : sessions) {
            assertThat(session.closed(), is(false));
        }

        facade.stop();

        for (Session session : sessions) {
            assertThat(session.closed(), is(true));
        }

        assertThat(facade.sessions(), empty());
    }

    @Test
    void itsImpossibleToCreateSessionsAfterFacadeIsStopped() throws Exception {
        IgniteSqlImpl facade = newSqlFacade();

        SessionBuilder builder = facade.sessionBuilder();

        Session templateSession = builder.build();

        facade.stop();

        IgniteTestUtils.assertThrows(
                IgniteException.class,
                builder::build,
                "Node is stopping"
        );

        IgniteTestUtils.assertThrows(
                IgniteException.class,
                () -> facade.sessionBuilder().build(),
                "Node is stopping"
        );

        IgniteTestUtils.assertThrows(
                IgniteException.class,
                facade::createSession,
                "Node is stopping"
        );

        IgniteTestUtils.assertThrows(
                IgniteException.class,
                () -> templateSession.toBuilder().build(),
                "Node is stopping"
        );

        assertThat(facade.sessions(), empty());
    }

    private static IgniteSqlImpl newSqlFacade() {
        QueryProcessor queryProcessor = mock(QueryProcessor.class);

        when(queryProcessor.createSession(any()))
                .thenAnswer(ignored -> new SessionId(UUID.randomUUID()));

        return new IgniteSqlImpl("test", queryProcessor, mock(IgniteTransactions.class));
    }
}
