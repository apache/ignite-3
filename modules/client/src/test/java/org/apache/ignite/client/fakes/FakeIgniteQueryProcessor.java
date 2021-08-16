package org.apache.ignite.client.fakes;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.processors.query.calcite.QueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.SqlCursor;

public class FakeIgniteQueryProcessor implements QueryProcessor {
    @Override public List<SqlCursor<List<?>>> query(String schemaName, String qry, Object... params) {
        return Collections.singletonList(new FakeCursor());
    }

    @Override public void start() {

    }

    @Override public void stop() throws Exception {

    }
}
