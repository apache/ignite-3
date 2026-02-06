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

package org.apache.ignite.client.handler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.junit.jupiter.api.Test;

/**
 * Tests for class {@link DdlBatchingSuggester}.
 */
public class DdlBatchingSuggesterTest {
    @Test
    void suggestionIsPrintedOnlyIfQueriesWereExecutedInRow() {
        AtomicInteger counter = new AtomicInteger();
        DdlBatchingSuggester suggester = new DdlBatchingSuggester(ignored -> counter.incrementAndGet());

        for (int i = 0; i < DdlBatchingSuggester.THRESHOLD - 1; i++) {
            suggester.accept(SqlQueryType.DDL);
        }

        suggester.accept(SqlQueryType.QUERY);

        assertThat(counter.get(), is(0));

        for (int i = 0; i < DdlBatchingSuggester.THRESHOLD; i++) {
            suggester.accept(SqlQueryType.DDL);
        }

        assertThat(counter.get(), is(1));
    }

    @Test
    void suggestionIsPrintedOnlyOnce() {
        AtomicInteger counter = new AtomicInteger();
        DdlBatchingSuggester suggester = new DdlBatchingSuggester(ignored -> counter.incrementAndGet());

        for (int i = 0; i < 200; i++) {
            suggester.accept(SqlQueryType.DDL);
        }

        assertThat(counter.get(), is(1));
    }
}
