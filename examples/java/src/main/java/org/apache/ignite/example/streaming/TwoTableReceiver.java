package org.apache.ignite.example.streaming;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.DataStreamerReceiverContext;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;

/** Custom receiver class that extracts data from the provided source and write it into two separate tables: Customers and Addresses */

public class TwoTableReceiver implements DataStreamerReceiver<Tuple, Void, Tuple> {

    public TwoTableReceiver() {
    }

    @Override
    public CompletableFuture<List<Tuple>> receive(List<Tuple> page, DataStreamerReceiverContext ctx, Void arg) {

        RecordView<Tuple> customersTable = ctx.ignite().tables().table("Customers").recordView();
        RecordView<Tuple> addressesTable = ctx.ignite().tables().table("Addresses").recordView();

        List<Tuple> results = new ArrayList<>(page.size());

        for (Tuple sourceItem : page) {
            /* For each source item, receiver extracts customer and address data and upserts it into respective tables */
            Tuple customer = Tuple.create()
                    .set("id", sourceItem.intValue("customerId"))
                    .set("name", sourceItem.stringValue("customerName"))
                    .set("addressId", sourceItem.intValue("addressId"));

            Tuple address = Tuple.create()
                    .set("id", sourceItem.intValue("addressId"))
                    .set("street", sourceItem.stringValue("street"))
                    .set("city", sourceItem.stringValue("city"));

            customersTable.upsert(null, customer);
            addressesTable.upsert(null, address);
        }

        return CompletableFuture.completedFuture(results);
    }
}
