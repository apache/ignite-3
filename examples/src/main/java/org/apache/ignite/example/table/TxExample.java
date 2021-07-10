package org.apache.ignite.example.table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.AsyncTransaction;

/**
 * The example of transactional deposit update.
 */
public class TxExample {
    public static void main(String[] args) throws IOException {
        Ignite ignite = IgnitionManager.start(
            "node-0",
            Files.readString(Path.of("config", "ignite-config.json")),
            Path.of("work")
        );

        //---------------------------------------------------------------------------------
        //
        // Creating a table. The API call below is the equivalent of the following DDL:
        //
        //     CREATE TABLE accounts (
        //         accountNumber INT PRIMARY KEY,
        //         firstName     VARCHAR,
        //         lastName      VARCHAR,
        //         balance       DOUBLE
        //     )
        //
        //---------------------------------------------------------------------------------

        Table accounts = ignite.tables().createTable("PUBLIC.accounts", tbl -> tbl
            .changeName("PUBLIC.accounts")
            .changeColumns(cols -> cols
                .create("0", c -> c.changeName("accountNumber").changeType(t -> t.changeType("int32")).changeNullable(false))
                .create("1", c -> c.changeName("firstName").changeType(t -> t.changeType("string")).changeNullable(true))
                .create("2", c -> c.changeName("lastName").changeType(t -> t.changeType("string")).changeNullable(true))
                .create("3", c -> c.changeName("balance").changeType(t -> t.changeType("double")).changeNullable(true))
            )
            .changeIndices(idxs -> idxs
                .create("PK", idx -> idx
                    .changeName("PK")
                    .changeType("PK")
                    .changeColumns(cols -> cols.create("0", c -> c.changeName("accountNumber").changeAsc(true)))
                )
            )
        );

        KeyValueBinaryView kvView = accounts.kvView();

        Tuple k1 = accounts.tupleBuilder().set("accountNumber", 123456).build();
        Tuple k2 = accounts.tupleBuilder().set("accountNumber", 432).build();

        // Sync API.
        ignite.transactions().doInTransaction(tx -> {
            Tuple v1 = kvView.get(k1);
            Tuple v2 = kvView.get(k2);

            double balance1 = v1.doubleValue("balance");
            kvView.put(k1, accounts.tupleBuilder().set("balance", balance1 - 100).build());

            double balance2 = v2.doubleValue("balance");
            kvView.put(k2, accounts.tupleBuilder().set("balance", balance2 + 100).build());

            tx.commit();
        });

        // Async API.
        ignite.transactions().beginAsync().
            thenCompose(tx -> tx.runAsync(() -> kvView.getAsync(k1)).
                thenCombine(tx.runAsync(() -> kvView.getAsync(k2)), (v1, v2) -> new Pair<>(v1, v2)).
                thenCompose(pair -> tx.runAsync(() -> {
                    double balance = pair.getFirst().doubleValue("balance");

                    return kvView.putAsync(k1, accounts.tupleBuilder().set("balance", balance - 100).build());
                }).thenCombine(tx.runAsync(() -> {
                    double balance = pair.getSecond().doubleValue("balance");

                    return kvView.putAsync(k2, accounts.tupleBuilder().set("balance", balance + 100).build());
                }), (v1, v2) -> null)).thenApply(ignore -> tx))
            .thenCompose(AsyncTransaction::commitAsync);
    }
}
