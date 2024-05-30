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

package org.apache.ignite.internal;

import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;

/**
 * Utilities to unwrap implementation objects from public API objects. They should be used whenever you need to get an
 * instance of an implementation class (like {@link TableImpl}) via public API; direct casting will not work as
 * public API objects might be wrapped in wrappers.
 *
 * <p>So, instead of
 *
 * <p>{@code
 *     TableImpl tableImpl = (TableImpl) ignite.tables().table("test");
 * }
 *
 * <p>use
 *
 * <p>{@code
 *     TableImpl tableImpl = TestWrappers.unwrapTableImpl(ignite.tables().table("test"));
 * }
 */
public class TestWrappers {
    /**
     * Unwraps {@link TableImpl} from a {@link Table}.
     *
     * @param table Table to unwrap.
     */
    public static TableImpl unwrapTableImpl(Table table) {
        return Wrappers.unwrap(table, TableImpl.class);
    }

    /**
     * Unwraps {@link TableViewInternal} from a {@link Table}.
     *
     * @param table Table to unwrap.
     */
    public static TableViewInternal unwrapTableViewInternal(Table table) {
        return Wrappers.unwrap(table, TableViewInternal.class);
    }

    /**
     * Unwraps {@link TableManager} from an {@link IgniteTables}.
     *
     * @param tables Tables to unwrap.
     */
    public static TableManager unwrapTableManager(IgniteTables tables) {
        return Wrappers.unwrap(tables, TableManager.class);
    }

    /**
     * Unwraps {@link IgniteTablesInternal} from an {@link IgniteTables}.
     *
     * @param tables Tables to unwrap.
     */
    public static IgniteTablesInternal unwrapIgniteTablesInternal(IgniteTables tables) {
        return Wrappers.unwrap(tables, IgniteTablesInternal.class);
    }

    /**
     * Unwraps {@link IgniteTransactionsImpl} from an {@link IgniteTransactions}.
     *
     * @param transactions Object to unwrap.
     */
    public static IgniteTransactionsImpl unwrapIgniteTransactionsImpl(IgniteTransactions transactions) {
        return Wrappers.unwrap(transactions, IgniteTransactionsImpl.class);
    }

    /**
     * Unwraps {@link Transaction} from an {@link Transaction}.
     *
     * @param tx Object to unwrap.
     */
    public static Transaction unwrapIgniteTransaction(Transaction tx) {
        return Wrappers.unwrap(tx, Transaction.class);
    }
}
