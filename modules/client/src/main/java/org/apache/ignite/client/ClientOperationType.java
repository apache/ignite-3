package org.apache.ignite.client;

import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.Transaction;

public enum ClientOperationType {
    /**
     * Get tables ({@link IgniteTables#tables()}).
     */
    TABLES_GET,

    /**
     * Get table ({@link IgniteTables#table(String)}).
     */
    TABLE_GET,

    /**
     * Upsert ({@link RecordView#upsert(Transaction, Object)}).
     */
    TUPLE_UPSERT,

    /**
     * Get ({@link RecordView#get(Transaction, Object)}).
     */
    TUPLE_GET,

    /**
     * Upsert (<see cref="IRecordView{T}.UpsertAllAsync"/>).
     */
    TUPLE_UPSERT_ALL,

    /**
     * Get All (<see cref="IRecordView{T}.GetAllAsync"/>).
     */
    TUPLE_GET_ALL,

    /**
     * Get and Upsert (<see cref="IRecordView{T}.GetAndUpsertAsync"/>).
     */
    TUPLE_GET_AND_UPSERT,

    /**
     * Insert (<see cref="IRecordView{T}.InsertAsync"/>).
     */
    TUPLE_INSERT,

    /**
     * Insert All (<see cref="IRecordView{T}.InsertAllAsync"/>).
     */
    TUPLE_INSERT_ALL,

    /**
     * Replace (<see cref="IRecordView{T}.ReplaceAsync(Apache.Ignite.Transactions.ITransaction?,T)"/>).
     */
    TUPLE_REPLACE,

    /**
     * Replace Exact (<see cref="IRecordView{T}.ReplaceAsync(Apache.Ignite.Transactions.ITransaction?,T, T)"/>).
     */
    TUPLE_REPLACE_EXACT,

    /**
     * Get and Replace (<see cref="IRecordView{T}.GetAndReplaceAsync"/>).
     */
    TUPLE_GET_AND_REPLACE,

    /**
     * Delete (<see cref="IRecordView{T}.DeleteAsync"/>).
     */
    TUPLE_DELETE,

    /**
     * Delete All (<see cref="IRecordView{T}.DeleteAllAsync"/>).
     */
    TUPLE_DELETE_ALL,

    /**
     * Delete Exact (<see cref="IRecordView{T}.DeleteExactAsync"/>).
     */
    TUPLE_DELETE_EXACT,

    /**
     * Delete All Exact (<see cref="IRecordView{T}.DeleteAllExactAsync"/>).
     */
    TUPLE_DELETE_ALL_EXACT,

    /**
     * Get and Delete (<see cref="IRecordView{T}.GetAndDeleteAsync"/>).
     */
    TUPLE_GET_AND_DELETE,

    COMPUTE_EXECUTE
}
