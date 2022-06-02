package org.apache.ignite.cli.commands.sql;

import java.util.List;
import org.apache.ignite.cli.sql.SchemaProvider;
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
                //TODO: add columns from current schema?
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
