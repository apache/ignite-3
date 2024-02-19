package org.apache.ignite.example;

import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;
import static org.apache.ignite.internal.tracing.TracingManager.span;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.tracing.TraceSpan;
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
    void delayTracing() {
        try (TraceSpan parentSpan = rootSpan("try-span")) {
            try (TraceSpan ignored = span("childSpan")){
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        rootSpan("closure-span", (parentSpan) -> {
           span("childSpan", (span) -> {
               try {
                   Thread.sleep(10L);
               } catch (InterruptedException e) {
                   throw new RuntimeException(e);
               }
           });

           return null;
        });
    }

    @Test
    void kvGetWithTracing() {
        KeyValueView<Tuple, Tuple> keyValueView = CLUSTER.aliveNode().tables().table(DEFAULT_TABLE_NAME).keyValueView();

        // Warm-up
        try (TraceSpan parentSpan = rootSpan("WarmSpan")) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        Tuple key = Tuple.create(Map.of("id", 0));

        var start = System.nanoTime();

        try (TraceSpan parentSpan = rootSpan("kvGetOperation")) {
            try (TraceSpan childSpan = span("kvGet")) {

            }
        }

        System.out.println(">>> " + (System.nanoTime() - start) / 1000L);
//
//        try (TraceSpan parentSpan = rootSpan("kvGetOperation")) {
//            keyValueView.get(null, key);
//        }
    }

    @Override
    protected int initialNodes() {
        return 1;
    }
}
