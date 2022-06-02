package org.apache.ignite.cli.commands.sql;

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