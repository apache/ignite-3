package org.apache.ignite.example;

import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;

import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class KvOperationTest extends ClusterPerClassIntegrationTest {
    @BeforeAll
    @Override
    protected void beforeAll(TestInfo testInfo) {
        super.beforeAll(testInfo);

        createZoneAndTable(zoneName(DEFAULT_TABLE_NAME), DEFAULT_TABLE_NAME, 1, 1);

        insertPeople(DEFAULT_TABLE_NAME, new Person(0, "0", 10.0));
    }

    @Test
    void kvGetWithTracing() {
        KeyValueView<Tuple, Tuple> keyValueView = CLUSTER.aliveNode().tables().table(DEFAULT_TABLE_NAME).keyValueView();

        rootSpan("kvGetOperation", (parentSpan) -> {
            keyValueView.get(null, Tuple.create().set("id", 0));

            return null;
        });
    }

    @Override
    protected int initialNodes() {
        return 1;
    }
}
