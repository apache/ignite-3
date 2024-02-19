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

package org.apache.ignite.internal.cli.commands.sql;

import java.util.List;
import org.apache.ignite.internal.cli.sql.SchemaProvider;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

class SqlCompleter implements Completer {
    private final SchemaProvider schemaProvider;

    SqlCompleter(SchemaProvider schemaProvider) {
        this.schemaProvider = schemaProvider;
    }

    @Override
    public void complete(LineReader reader, ParsedLine commandLine, List<Candidate> candidates) {
        if (commandLine.line().startsWith("help")) {
            return;
        }
        if (commandLine.wordIndex() == 0) {
            addCandidatesFromArray(SqlMetaData.STARTING_KEYWORDS, candidates);
        } else {
            fillCandidates(candidates);
        }
    }

    private void fillCandidates(List<Candidate> candidates) {
        addKeywords(candidates);
        for (String schema : schemaProvider.getSchema().schemas()) {
            addCandidate(schema, candidates);
            for (String table : schemaProvider.getSchema().tables(schema)) {
                addCandidate(table, candidates);
                // TODO: https://issues.apache.org/jira/browse/IGNITE-16973
            }
        }
    }

    private void addKeywords(List<Candidate> candidates) {
        addCandidatesFromArray(SqlMetaData.KEYWORDS, candidates);
        addCandidatesFromArray(SqlMetaData.NUMERIC_FUNCTIONS, candidates);
        addCandidatesFromArray(SqlMetaData.STRING_FUNCTIONS, candidates);
        addCandidatesFromArray(SqlMetaData.TIME_DATE_FUNCTIONS, candidates);
        addCandidatesFromArray(SqlMetaData.SYSTEM_FUNCTIONS, candidates);
    }

    private static void addCandidatesFromArray(String[] strings, List<Candidate> candidates) {
        for (String keyword : strings) {
            addCandidate(keyword, candidates);
        }
    }

    private static void addCandidate(String string, List<Candidate> candidates) {
        candidates.add(new Candidate(string));
        candidates.add(new Candidate(string.toLowerCase()));
    }
}
