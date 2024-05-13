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

package org.apache.ignite.internal.sql.engine.util.tpch;

import static org.apache.ignite.sql.ColumnType.DATE;
import static org.apache.ignite.sql.ColumnType.DECIMAL;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.STRING;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Iterator;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

/**
 * Enumeration of tables from TPC-H specification.
 */
public enum TpchTables {
    LINEITEM(
            new Column("L_ORDERKEY", INT32),
            new Column("L_PARTKEY", INT32),
            new Column("L_SUPPKEY", INT32),
            new Column("L_LINENUMBER", INT32),
            new Column("L_QUANTITY", DECIMAL),
            new Column("L_EXTENDEDPRICE", DECIMAL),
            new Column("L_DISCOUNT", DECIMAL),
            new Column("L_TAX", DECIMAL),
            new Column("L_RETURNFLAG", STRING),
            new Column("L_LINESTATUS", STRING),
            new Column("L_SHIPDATE", DATE),
            new Column("L_COMMITDATE", DATE),
            new Column("L_RECEIPTDATE", DATE),
            new Column("L_SHIPINSTRUCT", STRING),
            new Column("L_SHIPMODE", STRING),
            new Column("L_COMMENT", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::integer, ?::integer, ?::integer,"
                    + "?::decimal(15, 2), ?::decimal(15, 2), ?::decimal(15, 2),"
                    + "?::decimal(15, 2), ?::char(1), ?::char(1), ?::date,"
                    + "?::date, ?::date, ?::char(25), ?::char(10), ?::varchar(44));";
        }
    },

    PART(
            new Column("P_PARTKEY", INT32),
            new Column("P_NAME", STRING),
            new Column("P_MFGR", STRING),
            new Column("P_BRAND", STRING),
            new Column("P_TYPE", STRING),
            new Column("P_SIZE", INT32),
            new Column("P_CONTAINER", STRING),
            new Column("P_RETAILPRICE", DECIMAL),
            new Column("P_COMMENT", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::varchar(55), ?::char(25),"
                    + "?::char(10), ?::varchar(25), ?::integer,"
                    + "?::char(10), ?::decimal(15, 2), ?::varchar(23));";
        }
    },

    SUPPLIER(
            new Column("S_SUPPKEY", INT32),
            new Column("S_NAME", STRING),
            new Column("S_ADDRESS", STRING),
            new Column("S_NATIONKEY", INT32),
            new Column("S_PHONE", STRING),
            new Column("S_ACCTBAL", DECIMAL),
            new Column("S_COMMENT", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::char(25), ?::varchar(40),"
                    + "?::integer, ?::char(15), ?::decimal(15, 2), ?::varchar(101));";
        }
    },

    PARTSUPP(
            new Column("PS_PARTKEY", INT32),
            new Column("PS_SUPPKEY", INT32),
            new Column("PS_AVAILQTY", INT32),
            new Column("PS_SUPPLYCOST", DECIMAL),
            new Column("PS_COMMENT", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::integer, ?::integer,"
                    + "?::decimal(15, 2), ?::varchar(199));";
        }
    },

    NATION(
            new Column("N_NATIONKEY", INT32),
            new Column("N_NAME", STRING),
            new Column("N_REGIONKEY", INT32),
            new Column("N_COMMENT", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::char(25), ?::integer, ?::varchar(152));";
        }
    },

    REGION(
            new Column("R_REGIONKEY", INT32),
            new Column("R_NAME", STRING),
            new Column("R_COMMENT", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::char(25), ?::varchar(152));";
        }
    },

    ORDERS(
            new Column("O_ORDERKEY", INT32),
            new Column("O_CUSTKEY", INT32),
            new Column("O_ORDERSTATUS", STRING),
            new Column("O_TOTALPRICE", DECIMAL),
            new Column("O_ORDERDATE", DATE),
            new Column("O_ORDERPRIORITY", STRING),
            new Column("O_CLERK", STRING),
            new Column("O_SHIPPRIORITY", INT32),
            new Column("O_COMMENT", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::integer, ?::char(1),"
                    + "?::decimal(15, 2), ?::date, ?::char(15),"
                    + "?::char(15), ?::integer, ?::varchar(79));";
        }
    },

    CUSTOMER(
            new Column("C_CUSTKEY", INT32),
            new Column("C_NAME", STRING),
            new Column("C_ADDRESS", STRING),
            new Column("C_NATIONKEY", INT32),
            new Column("C_PHONE", STRING),
            new Column("C_ACCTBAL", DECIMAL),
            new Column("C_MKTSEGMENT", STRING),
            new Column("C_COMMENT", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::varchar(25), ?::varchar(40),"
                    + "?::integer, ?::char(15), ?::decimal(15, 2),"
                    + "?::char(10), ?::varchar(117));";
        }
    };

    private final Column[] columns;

    TpchTables(Column... columns) {
        this.columns = columns;
    }

    /** Returns name of the table. */
    public String tableName() {
        return name().toLowerCase();
    }

    /** Returns number of column in the table. */
    public int columnsCount() {
        return columns.length;
    }

    public String columnName(int idx) {
        return columns[idx].name;
    }

    /** Returns definition of a table including necessary indexes. */
    public String ddlScript() {
        return TpchHelper.loadFromResource("tpch/ddl/" + tableName() + "_ddl.sql");
    }

    /**
     * Returns DML string representing single-row INSERT statement with dynamic
     * parameters placeholders.
     *
     * <p>The order of columns matches the order specified in TPC-H specification
     * (see table declarations or output of {@link #ddlScript()}).
     *
     * <p>The statement returned is tolerant to columns' type mismatch, implying you
     * can use any value while there is cast from provided value to required type.
     */
    public abstract String insertPrepareStatement();

    /**
     * Returns iterator returning rows of the corresponding table.
     *
     * <p>May be used to fill the table via {@link KeyValueView KV API} or {@link IgniteSql SQL API}.
     *
     * @param pathToDataset A path to a directory with CSV file containing data for the table.
     * @return Iterator over data of the table.
     * @throws IOException In case of error.
     */
    public Iterator<Object[]> dataProvider(Path pathToDataset) throws IOException {
        return Files.lines(pathToDataset.resolve(tableName() + ".tbl"))
                .map(this::csvLineToTableValues)
                .iterator();
    }

    private Object[] csvLineToTableValues(String line) {
        String[] stringValues = line.split("\\|");
        Object[] values = new Object[columns.length];

        for (int i = 0; i < columns.length; i++) {
            switch (columns[i].type) {
                case INT32:
                    values[i] = Integer.valueOf(stringValues[i]);
                    break;
                case DECIMAL:
                    values[i] = new BigDecimal(stringValues[i]);
                    break;
                case DATE:
                    values[i] = LocalDate.parse(stringValues[i]);
                    break;
                case STRING:
                    values[i] = stringValues[i];
                    break;
                default:
                    throw new IllegalStateException(columns[i].type.toString());
            }
        }

        return values;
    }

    private static class Column {
        private final String name;
        private final ColumnType type;

        private Column(String name, ColumnType type) {
            this.name = name;
            this.type = type;
        }
    }
}
