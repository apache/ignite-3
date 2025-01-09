
package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.type.NativeTypes.stringOf;

import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryExecutor;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.SubscriptionUtils;

public class QueryHistoryViewProvider {
    private final CompletableFuture<QueryExecutor> queryExecutorFuture = new CompletableFuture<>();

    public void init(QueryExecutor queryExecutor) {
        queryExecutorFuture.complete(queryExecutor);
    }

    public SystemView<?> get() {
        Publisher<Entry<Long, String>> viewDataPublisher = SubscriptionUtils.fromIterable(
                queryExecutorFuture.thenApply(queryExecutor -> () -> queryExecutor.queryHistory().entrySet().iterator())
        );

        NativeType stringType = stringOf(Short.MAX_VALUE);

        return SystemViews.<Entry<Long, String>>nodeViewBuilder()
                .name("SQL_QUERY_HISTORY")
                .addColumn("LENGTH", NativeTypes.INT64, Entry::getKey)
                .addColumn("SQL", stringType, Entry::getValue)
                .dataProvider(viewDataPublisher)
                .build();
    }
}
