package org.apache.ignite.client.fakes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.processors.query.calcite.SqlCursor;
import org.apache.ignite.internal.processors.query.calcite.SqlQueryType;

public class FakeCursor implements SqlCursor<List<?>> {

    private final Random random;

    FakeCursor() {
        random = new Random();
    }

    @Override public void close() throws Exception {

    }

    @Override public Iterator<List<?>> iterator() {
        return null;
    }

    @Override public boolean hasNext() {
        return true;
    }

    @Override public List<?> next() {
        List<Object> result = new ArrayList<>();
        result.add(random.nextInt());
        result.add(random.nextLong());
        result.add(random.nextFloat());
        result.add(random.nextDouble());
        result.add(UUID.randomUUID().toString());
        result.add(null);

        return result;
    }

    @Override public SqlQueryType getQueryType() {
        return SqlQueryType.QUERY;
    }
}
