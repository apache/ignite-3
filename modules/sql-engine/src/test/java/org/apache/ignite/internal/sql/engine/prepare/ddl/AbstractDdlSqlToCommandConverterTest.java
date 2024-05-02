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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.UUID;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.generated.query.calcite.sql.IgniteSqlParserImpl;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.hamcrest.Matchers;

/**
 * Common methods for {@link DdlSqlToCommandConverter} testing.
 */
class AbstractDdlSqlToCommandConverterTest extends BaseIgniteAbstractTest {
    /** DDL SQL to command converter. */
    final DdlSqlToCommandConverter converter = new DdlSqlToCommandConverter();

    final Catalog catalog = mock(Catalog.class);

    /**
     * Parses a given statement and returns a resulting AST.
     *
     * @param stmt Statement to parse.
     * @return An AST.
     */
    static SqlNode parse(String stmt) throws SqlParseException {
        SqlParser parser = SqlParser.create(stmt, SqlParser.config().withParserFactory(IgniteSqlParserImpl.FACTORY));

        return parser.parseStmt();
    }

    static PlanningContext createContext() {
        var schemaName = CatalogService.DEFAULT_SCHEMA_NAME;
        IgniteSchema publicSchema = new IgniteSchema(schemaName, 1, List.of());
        var schema = Frameworks.createRootSchema(false).add(schemaName, publicSchema);

        return PlanningContext.builder()
                .parentContext(BaseQueryContext.builder()
                        .queryId(UUID.randomUUID())
                        .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(schema)
                                .build())
                        .build())
                .query("")
                .build();
    }

    static <T> T castFirstEntry(List<UpdateEntry> entries, Class<T> expected) {
        assertThat(entries, not(empty()));

        UpdateEntry entry = entries.get(0);

        assertThat(entry, Matchers.instanceOf(expected));

        return (T) entry;
    }
}
