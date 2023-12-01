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

package org.apache.ignite.internal.sql.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Set;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.junit.jupiter.api.Test;

class SqlNodeUtilsTest {
    private final ParserService parserService = new ParserServiceImpl(100, CaffeineCacheFactory.INSTANCE);

    @Test
    void extractsTableNamesFromSelectFrom() {
        assertThat(extractTableNames("select x from a, b as x"), containsInAnyOrder("A", "B"));
    }

    @Test
    void extractsTableNamesFromSelectJoin() {
        assertThat(
                extractTableNames("select x from a, b as x join c on c1=c2 left join d on c3=c4"),
                containsInAnyOrder("A", "B", "C", "D")
        );
    }

    @Test
    void extractsTableNamesFromSelectWithSubqueryInProjection() {
        assertThat(extractTableNames("select x + (select count(y) from b) from a as y"), containsInAnyOrder("A", "B"));
    }

    @Test
    void extractsTableNamesFromSelectWithSubqueryInFrom() {
        assertThat(extractTableNames("select x from (select y from b) t"), containsInAnyOrder("B"));
    }

    @Test
    void extractsTableNamesFromSelectWithSubqueryInJoin() {
        assertThat(extractTableNames("select x from a, (select y from b) t"), containsInAnyOrder("A", "B"));
    }

    @Test
    void extractsTableNamesFromSelectWithSubqueryInWhere() {
        assertThat(extractTableNames("select x from a as w where (select count(y) from b) > 0"), containsInAnyOrder("A", "B"));
    }

    @Test
    void extractsTableNamesFromSelectWithSubqueryInGroupBy() {
        assertThat(extractTableNames("select x from a group by (select count(y) from b)"), containsInAnyOrder("A", "B"));
    }

    @Test
    void extractsTableNamesFromSelectWithSubqueryInHaving() {
        assertThat(extractTableNames("select x from a group by y having (select count(z) from b) > 0"), containsInAnyOrder("A", "B"));
    }

    @Test
    void extractsTableNameFromInsertWithValues() {
        assertThat(extractTableNames("insert into a (id) values (1)"), containsInAnyOrder("A"));
    }

    @Test
    void extractsTableNameFromInsertWithSelect() {
        assertThat(extractTableNames("insert into a (id) select id from b where z = w"), containsInAnyOrder("A", "B"));
    }

    @Test
    void extractsTableNameFromUpdate() {
        assertThat(extractTableNames("update a set x = y where z = w"), containsInAnyOrder("A"));
    }

    @Test
    void extractsTableNameFromDelete() {
        assertThat(extractTableNames("delete from a where z = w"), containsInAnyOrder("A"));
    }

    @Test
    void extractsTableNameFromMerge() {
        assertThat(
                extractTableNames("merge into a using b on x = y "
                        + "when matched then update set z = w "
                        + "when not matched then insert values (default)"),
                containsInAnyOrder("A", "B")
        );
    }

    private Set<String> extractTableNames(String sql) {
        ParsedResult parsedResult = parserService.parse(sql);
        return SqlNodeUtils.extractTableNames(parsedResult.parsedTree());
    }
}
