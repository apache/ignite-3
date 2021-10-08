package org.apache.ignite.example.rebalance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

public class RebalanceExample {

    public static void main(String[] args) throws Exception {
        var nodes = new ArrayList<Ignite>();

        for (int i = 0; i < 3; i++) {
            nodes.add(IgnitionManager.start("node-" + i,
                Files.readString(Path.of("config", "rebalance", "ignite-config-" + i + ".json")),
                Path.of("work" + i)));
        }

        var node0 = nodes.get(0);

        Table testTable = node0.tables().createTable("PUBLIC.rebalance", tbl ->
            SchemaConfigurationConverter.convert(
                SchemaBuilders.tableBuilder("PUBLIC", "rebalance")
                    .columns(
                        SchemaBuilders.column("key", ColumnType.INT32).asNonNull().build(),
                        SchemaBuilders.column("value", ColumnType.string()).asNullable().build()
                    )
                    .withPrimaryKey("key")
                    .build(), tbl)
                .changeReplicas(5)
                .changePartitions(1)
        );

        KeyValueView<Tuple, Tuple> kvView = testTable.keyValueView();
        Tuple key = Tuple.create()
            .set("key", 1);

        Tuple value = Tuple.create()
            .set("value", "test");

        kvView.put(key, value);

        value = testTable.recordView().get(key).value("value");
        System.out.println("Value on "

        for (int i = 3; i < 5; i++) {
            nodes.add(IgnitionManager.start("node-" + i,
                Files.readString(Path.of("config", "rebalance", "ignite-config-" + i + ".json")),
                Path.of("work" + i)));
        }

        node0.setBaseline(Set.of("node-0", "node-3", "node-4"));

        IgnitionManager.stop(nodes.get(1).name());
        IgnitionManager.stop(nodes.get(2).name());

        var valueOnNewNode = nodes.get(4).tables().table("PUBLIC.rebalance").recordView().get(key).value("value");

        System.out.println("Value on new node " + valueOnNewNode);
        System.exit(0);
    }
}
