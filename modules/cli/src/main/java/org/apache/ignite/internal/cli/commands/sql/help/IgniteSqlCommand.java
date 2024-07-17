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

package org.apache.ignite.internal.cli.commands.sql.help;

import java.util.Arrays;
import java.util.Optional;

/** Enum of Ignite SQL commands. */
public enum IgniteSqlCommand {
    SELECT("SELECT",
            "SELECT [ hintComment ] [ STREAM ] [ ALL | DISTINCT ]\n"
                    + "    { * | projectItem [, projectItem ]* }\n"
                    + "FROM tableExpression\n"
                    + "[ WHERE booleanExpression ]\n"
                    + "[ GROUP BY { groupItem [, groupItem ]* } ]\n"
                    + "[ HAVING booleanExpression ]"),
    select("select", SELECT.syntax),

    INSERT("INSERT",
            "INSERT INTO tableName\n"
                    + "[ '(' column [, column ]* ')' ]"),
    insert("insert", INSERT.syntax),

    UPDATE("UPDATE",
            "UPDATE tableName\n"
                    + "SET assign [, assign ]*\n"
                    + "[ WHERE booleanExpression ]"),
    update("update", UPDATE.syntax),

    DELETE("DELETE",
            "DELETE FROM tableName [ [ AS ] alias ]\n"
                    + "[ WHERE booleanExpression ]"),
    delete("delete", DELETE.syntax),

    CREATE_TABLE("CREATE TABLE",
            "CREATE TABLE [IF NOT EXISTS] tableName (tableColumn [, tableColumn]...)\n"
                    + "[COLOCATE [BY] (columnName [, columnName]...)]\n"
                    + "[ENGINE engineName]\n"
                    + "[WITH paramName=paramValue [,paramName=paramValue]...]\n"
                    + "tableColumn = columnName columnType [[NOT] NULL] [DEFAULT defaultValue] [PRIMARY KEY]"),
    create_table("create table", CREATE_TABLE.syntax),

    DROP_TABLE("DROP TABLE",
            "DROP TABLE IF EXISTS tableName"),
    drop_table("drop table", DROP_TABLE.syntax),

    ALTER_TABLE("ALTER TABLE",
            "ALTER TABLE tableName ADD COLUMN (tableColumn [, tableColumn]...) \n\n"
                    + "ALTER TABLE tableName DROP COLUMN (tableColumn [, tableColumn]...)"),
    alter_table("alter table", ALTER_TABLE.syntax),

    CREATE_INDEX("CREATE INDEX",
            "CREATE INDEX [IF NOT EXISTS] name ON tableName\n"
                    + "[USING { HASH | SORTED }]\n"
                    + "(column_definition [, column_definition]...)"),
    create_index("create index", CREATE_INDEX.syntax),

    DROP_INDEX("DROP INDEX",
            "DROP INDEX [IF EXISTS] indexName"),
    drop_index("drop index", DROP_INDEX.syntax);

    private final String topic;
    private final String syntax;

    IgniteSqlCommand(String topic, String syntax) {
        this.topic = topic;
        this.syntax = syntax;
    }

    /** Finds a SQL command by a topic. */
    static Optional<IgniteSqlCommand> find(String topic) {
        return Arrays.stream(values())
                .filter(it -> it.getTopic().equalsIgnoreCase(topic))
                .findFirst();
    }

    String getTopic() {
        return topic;
    }

    String getSyntax() {
        return syntax;
    }
}
