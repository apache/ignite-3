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

/**
 * Enumeration of tables from TPC-H specification.
 */
public enum TpchTables {
    LINEITEM {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::integer, ?::integer, ?::integer,"
                    + "?::decimal(15, 2), ?::decimal(15, 2), ?::decimal(15, 2),"
                    + "?::decimal(15, 2), ?::char(1), ?::char(1), ?::date,"
                    + "?::date, ?::date, ?::char(25), ?::char(10), ?::varchar(44));";
        }
    },

    PART {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::varchar(55), ?::char(25),"
                    + "?::char(10), ?::varchar(25), ?::integer,"
                    + "?::char(10), ?::decimal(15, 2), ?::varchar(23));";
        }
    },

    SUPPLIER {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::char(25), ?::varchar(40),"
                    + "?::integer, ?::char(15), ?::decimal(15, 2), ?::varchar(101));";
        }
    },

    PARTSUPP {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::integer, ?::integer,"
                    + "?::decimal(15, 2), ?::varchar(199));";
        }
    },

    NATION {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::char(25), ?::integer, ?::varchar(152));";
        }
    },

    REGION {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::char(25), ?::varchar(152));";
        }
    },

    ORDERS {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::integer, ?::char(1),"
                    + "?::decimal(15, 2), ?::date, ?::char(15),"
                    + "?::char(15), ?::integer, ?::varchar(79));";
        }
    },

    CUSTOMER {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::varchar(25), ?::varchar(40),"
                    + "?::integer, ?::char(15), ?::decimal(15, 2),"
                    + "?::char(10), ?::varchar(117));";
        }
    };

    /** Returns name of the table. */
    public String tableName() {
        return name().toLowerCase();
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
    public String insertPrepareStatement() {
        throw new AssertionError("`insertPrepareStatement` must be overriden");
    }
}
