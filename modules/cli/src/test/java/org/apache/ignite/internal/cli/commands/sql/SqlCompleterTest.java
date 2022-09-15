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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.jline.reader.Candidate;
import org.jline.reader.impl.DefaultParser;
import org.junit.jupiter.api.Test;

class SqlCompleterTest {

    @Test
    void testCandidatesForEmptyLineDontContainExtraKeywordsOrSchema() {
        SqlCompleter completer = new SqlCompleter(new SchemaProviderMock());
        List<Candidate> candidates = new ArrayList<>();
        completer.complete(null,
                new DefaultParser().new ArgumentList(
                        "",
                        List.of(""),
                        0,
                        0,
                        0
                ),
                candidates
        );
        assertThat(candidates).doesNotContain(new Candidate("ABS"), new Candidate("ASCII"), new Candidate("PERSON"));
    }

    @Test
    void testCandidatesForSecondWordContainExtraKeywordsAndSchema() {
        SqlCompleter completer = new SqlCompleter(new SchemaProviderMock());
        List<Candidate> candidates = new ArrayList<>();
        completer.complete(null,
                new DefaultParser().new ArgumentList(
                        "select",
                        List.of("select", ""),
                        1,
                        0,
                        6
                ),
                candidates
        );
        assertThat(candidates).contains(new Candidate("ABS"), new Candidate("ASCII"), new Candidate("PERSON"));
    }
}
