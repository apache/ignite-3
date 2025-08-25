/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.example.streaming;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.DataStreamerReceiverContext;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;

/** Custom receiver class that extracts data from the provided source and write it into two separate tables: Customers and Addresses */

public class TwoTableReceiver implements DataStreamerReceiver<Tuple, Tuple, Void> {
    @Override
    public CompletableFuture<List<Void>> receive(List<Tuple> page, DataStreamerReceiverContext ctx, Tuple arg) {
        RecordView<Tuple> customersTable = ctx.ignite().tables().table("Customers").recordView();
        RecordView<Tuple> addressesTable = ctx.ignite().tables().table("Addresses").recordView();

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

        return CompletableFuture.completedFuture(List.of());
    }
}