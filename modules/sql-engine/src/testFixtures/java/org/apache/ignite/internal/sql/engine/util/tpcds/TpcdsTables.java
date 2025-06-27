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

package org.apache.ignite.internal.sql.engine.util.tpcds;

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
import org.apache.ignite.internal.sql.engine.util.TpcScaleFactor;
import org.apache.ignite.internal.sql.engine.util.TpcTable;
import org.apache.ignite.internal.sql.engine.util.tpch.TpchHelper;
import org.apache.ignite.sql.ColumnType;

/**
 * Enumeration of tables from TPC-DS specification.
 */
@SuppressWarnings("NonSerializableFieldInSerializableClass")
public enum TpcdsTables implements TpcTable {
    CUSTOMER(
            new long[] {100_000, 12_000_000, 30_000_000, 65_000_000, 80_000_000, 100_000_000},
            new Column("C_CUSTOMER_SK", INT32),
            new Column("C_CUSTOMER_ID", STRING),
            new Column("C_CURRENT_CDEMO_SK", INT32),
            new Column("C_CURRENT_HDEMO_SK", INT32),
            new Column("C_CURRENT_ADDR_SK", INT32),
            new Column("C_FIRST_SHIPTO_DATE_SK", INT32),
            new Column("C_FIRST_SALES_DATE_SK", INT32),
            new Column("C_SALUTATION", STRING),
            new Column("C_FIRST_NAME", STRING),
            new Column("C_LAST_NAME", STRING),
            new Column("C_PREFERRED_CUST_FLAG", STRING),
            new Column("C_BIRTH_DAY", INT32),
            new Column("C_BIRTH_MONTH", INT32),
            new Column("C_BIRTH_YEAR", INT32),
            new Column("C_BIRTH_COUNTRY", STRING),
            new Column("C_LOGIN", STRING),
            new Column("C_EMAIL_ADDRESS", STRING),
            new Column("C_LAST_REVIEW_DATE_SK", INT32)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::integer, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::varchar(10), ?::varchar(20), ?::varchar(30), ?::varchar," 
                    + " ?::integer, ?::integer, ?::integer, ?::varchar(20), ?::varchar(13), ?::varchar(50), ?::integer);";
        }
    },

    WEB_PAGE(
            new long[] {60, 3_000, 3_600, 4_002, 4_602, 5_004},
            new Column("WP_WEB_PAGE_SK", INT32),
            new Column("WP_WEB_PAGE_ID", STRING),
            new Column("WP_REC_START_DATE", DATE),
            new Column("WP_REC_END_DATE", DATE),
            new Column("WP_CREATION_DATE_SK", INT32),
            new Column("WP_ACCESS_DATE_SK", INT32),
            new Column("WP_AUTOGEN_FLAG", STRING),
            new Column("WP_CUSTOMER_SK", INT32),
            new Column("WP_URL", STRING),
            new Column("WP_TYPE", STRING),
            new Column("WP_CHAR_COUNT", INT32),
            new Column("WP_LINK_COUNT", INT32),
            new Column("WP_IMAGE_COUNT", INT32),
            new Column("WP_MAX_AD_COUNT", INT32)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::date, ?::date, ?::integer," 
                    + " ?::integer, ?::varchar, ?::integer, ?::varchar(100), ?::varchar(50), ?::integer," 
                    + " ?::integer, ?::integer, ?::integer);";
        }
    },

    STORE_SALES(
            new long[] {2_880_404, 2_879_987_999L, 8_639_936_081L, 28_799_983_563L, 86_399_341_874L, 287_997_818_084L},
            new Column("SS_SOLD_DATE_SK", INT32),
            new Column("SS_SOLD_TIME_SK", INT32),
            new Column("SS_ITEM_SK", INT32),
            new Column("SS_CUSTOMER_SK", INT32),
            new Column("SS_CDEMO_SK", INT32),
            new Column("SS_HDEMO_SK", INT32),
            new Column("SS_ADDR_SK", INT32),
            new Column("SS_STORE_SK", INT32),
            new Column("SS_PROMO_SK", INT32),
            new Column("SS_TICKET_NUMBER", INT32),
            new Column("SS_QUANTITY", INT32),
            new Column("SS_WHOLESALE_COST", DECIMAL),
            new Column("SS_LIST_PRICE", DECIMAL),
            new Column("SS_SALES_PRICE", DECIMAL),
            new Column("SS_EXT_DISCOUNT_AMT", DECIMAL),
            new Column("SS_EXT_SALES_PRICE", DECIMAL),
            new Column("SS_EXT_WHOLESALE_COST", DECIMAL),
            new Column("SS_EXT_LIST_PRICE", DECIMAL),
            new Column("SS_EXT_TAX", DECIMAL),
            new Column("SS_COUPON_AMT", DECIMAL),
            new Column("SS_NET_PAID", DECIMAL),
            new Column("SS_NET_PAID_INC_TAX", DECIMAL),
            new Column("SS_NET_PROFIT", DECIMAL)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::numeric(7, 2)," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2)," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2)," 
                    + " ?::numeric(7, 2));";
        }
    },

    HOUSEHOLD_DEMOGRAPHICS(
            new long[] {7_200, 7_200, 7_200, 7_200, 7_200, 7_200},
            new Column("HD_DEMO_SK", INT32),
            new Column("HD_INCOME_BAND_SK", INT32),
            new Column("HD_BUY_POTENTIAL", STRING),
            new Column("HD_DEP_COUNT", INT32),
            new Column("HD_VEHICLE_COUNT", INT32)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::integer, ?::varchar(15)," 
                    + " ?::integer, ?::integer);";
        }
    },

    CATALOG_PAGE(
            new long[] {11_718, 30_000, 36_000, 40_000, 46_000, 50_000},
            new Column("CP_CATALOG_PAGE_SK", INT32),
            new Column("CP_CATALOG_PAGE_ID", STRING),
            new Column("CP_START_DATE_SK", INT32),
            new Column("CP_END_DATE_SK", INT32),
            new Column("CP_DEPARTMENT", STRING),
            new Column("CP_CATALOG_NUMBER", INT32),
            new Column("CP_CATALOG_PAGE_NUMBER", INT32),
            new Column("CP_DESCRIPTION", STRING),
            new Column("CP_TYPE", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::integer, ?::integer," 
                    + " ?::varchar(50), ?::integer, ?::integer, ?::varchar(100), ?::varchar(100));";
        }
    },

    WEB_SITE(
            new long[] {30, 54, 66, 78, 84, 96},
            new Column("WEB_SITE_SK", INT32),
            new Column("WEB_SITE_ID", STRING),
            new Column("WEB_REC_START_DATE", DATE),
            new Column("WEB_REC_END_DATE", DATE),
            new Column("WEB_NAME", STRING),
            new Column("WEB_OPEN_DATE_SK", INT32),
            new Column("WEB_CLOSE_DATE_SK", INT32),
            new Column("WEB_CLASS", STRING),
            new Column("WEB_MANAGER", STRING),
            new Column("WEB_MKT_ID", INT32),
            new Column("WEB_MKT_CLASS", STRING),
            new Column("WEB_MKT_DESC", STRING),
            new Column("WEB_MARKET_MANAGER", STRING),
            new Column("WEB_COMPANY_ID", INT32),
            new Column("WEB_COMPANY_NAME", STRING),
            new Column("WEB_STREET_NUMBER", STRING),
            new Column("WEB_STREET_NAME", STRING),
            new Column("WEB_STREET_TYPE", STRING),
            new Column("WEB_SUITE_NUMBER", STRING),
            new Column("WEB_CITY", STRING),
            new Column("WEB_COUNTY", STRING),
            new Column("WEB_STATE", STRING),
            new Column("WEB_ZIP", STRING),
            new Column("WEB_COUNTRY", STRING),
            new Column("WEB_GMT_OFFSET", DECIMAL),
            new Column("WEB_TAX_PERCENTAGE", DECIMAL)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::date, ?::date, ?::varchar(50)," 
                    + " ?::integer, ?::integer, ?::varchar(50), ?::varchar(40), ?::integer, ?::varchar(50)," 
                    + " ?::varchar(100), ?::varchar(40), ?::integer, ?::varchar(50), ?::varchar(10), ?::varchar(60)," 
                    + " ?::varchar(15), ?::varchar(10), ?::varchar(60), ?::varchar(30), ?::varchar(2)," 
                    + " ?::varchar(10), ?::varchar(20), ?::numeric(5, 2), ?::numeric(5, 2));";
        }
    },

    CUSTOMER_DEMOGRAPHICS(
            new long[] {1_920_800, 1_920_800, 1_920_800, 1_920_800, 1_920_800, 1_920_800},
            new Column("CD_DEMO_SK", INT32),
            new Column("CD_GENDER", STRING),
            new Column("CD_MARITAL_STATUS", STRING),
            new Column("CD_EDUCATION_STATUS", STRING),
            new Column("CD_PURCHASE_ESTIMATE", INT32),
            new Column("CD_CREDIT_RATING", STRING),
            new Column("CD_DEP_COUNT", INT32),
            new Column("CD_DEP_EMPLOYED_COUNT", INT32),
            new Column("CD_DEP_COLLEGE_COUNT", INT32)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar, ?::varchar, ?::varchar(20)," 
                    + " ?::integer, ?::varchar(10), ?::integer, ?::integer, ?::integer);";
        }
    },

    ITEM(
            new long[] {18_000, 300_000, 360_000, 402_000, 462_000, 502_000},
            new Column("I_ITEM_SK", INT32),
            new Column("I_ITEM_ID", STRING),
            new Column("I_REC_START_DATE", DATE),
            new Column("I_REC_END_DATE", DATE),
            new Column("I_ITEM_DESC", STRING),
            new Column("I_CURRENT_PRICE", DECIMAL),
            new Column("I_WHOLESALE_COST", DECIMAL),
            new Column("I_BRAND_ID", INT32),
            new Column("I_BRAND", STRING),
            new Column("I_CLASS_ID", INT32),
            new Column("I_CLASS", STRING),
            new Column("I_CATEGORY_ID", INT32),
            new Column("I_CATEGORY", STRING),
            new Column("I_MANUFACT_ID", INT32),
            new Column("I_MANUFACT", STRING),
            new Column("I_SIZE", STRING),
            new Column("I_FORMULATION", STRING),
            new Column("I_COLOR", STRING),
            new Column("I_UNITS", STRING),
            new Column("I_CONTAINER", STRING),
            new Column("I_MANAGER_ID", INT32),
            new Column("I_PRODUCT_NAME", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::date, ?::date, ?::varchar(200)," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::integer, ?::varchar(50), ?::integer," 
                    + " ?::varchar(50), ?::integer, ?::varchar(50), ?::integer, ?::varchar(50), ?::varchar(20)," 
                    + " ?::varchar(20), ?::varchar(20), ?::varchar(10), ?::varchar(10), ?::integer, ?::varchar(50));";
        }
    },

    WAREHOUSE(
            new long[] {5, 20, 22, 25, 27, 30},
            new Column("W_WAREHOUSE_SK", INT32),
            new Column("W_WAREHOUSE_ID", STRING),
            new Column("W_WAREHOUSE_NAME", STRING),
            new Column("W_WAREHOUSE_SQ_FT", INT32),
            new Column("W_STREET_NUMBER", STRING),
            new Column("W_STREET_NAME", STRING),
            new Column("W_STREET_TYPE", STRING),
            new Column("W_SUITE_NUMBER", STRING),
            new Column("W_CITY", STRING),
            new Column("W_COUNTY", STRING),
            new Column("W_STATE", STRING),
            new Column("W_ZIP", STRING),
            new Column("W_COUNTRY", STRING),
            new Column("W_GMT_OFFSET", DECIMAL)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::varchar(20), ?::integer," 
                    + " ?::varchar(10), ?::varchar(60), ?::varchar(15), ?::varchar(10), ?::varchar(60)," 
                    + " ?::varchar(30), ?::varchar(2), ?::varchar(10), ?::varchar(20), ?::numeric(5, 2));";
        }
    },

    STORE_RETURNS(
            new long[] {287_514, 287_999_764, 863_989_652, 2_879_970_104L, 8_639_952_111L, 28_800_018_820L},
            new Column("SR_RETURNED_DATE_SK", INT32),
            new Column("SR_RETURN_TIME_SK", INT32),
            new Column("SR_ITEM_SK", INT32),
            new Column("SR_CUSTOMER_SK", INT32),
            new Column("SR_CDEMO_SK", INT32),
            new Column("SR_HDEMO_SK", INT32),
            new Column("SR_ADDR_SK", INT32),
            new Column("SR_STORE_SK", INT32),
            new Column("SR_REASON_SK", INT32),
            new Column("SR_TICKET_NUMBER", INT32),
            new Column("SR_RETURN_QUANTITY", INT32),
            new Column("SR_RETURN_AMT", DECIMAL),
            new Column("SR_RETURN_TAX", DECIMAL),
            new Column("SR_RETURN_AMT_INC_TAX", DECIMAL),
            new Column("SR_FEE", DECIMAL),
            new Column("SR_RETURN_SHIP_COST", DECIMAL),
            new Column("SR_REFUNDED_CASH", DECIMAL),
            new Column("SR_REVERSED_CHARGE", DECIMAL),
            new Column("SR_STORE_CREDIT", DECIMAL),
            new Column("SR_NET_LOSS", DECIMAL)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2)," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2));";
        }
    },

    SHIP_MODE(
            new long[] {20, 20, 20, 20, 20, 20},
            new Column("SM_SHIP_MODE_SK", INT32),
            new Column("SM_SHIP_MODE_ID", STRING),
            new Column("SM_TYPE", STRING),
            new Column("SM_CODE", STRING),
            new Column("SM_CARRIER", STRING),
            new Column("SM_CONTRACT", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::varchar(30)," 
                    + " ?::varchar(10), ?::varchar(20), ?::varchar(20));";
        }
    },

    INCOME_BAND(
            new long[] {20, 20, 20, 20, 20, 20},
            new Column("IB_INCOME_BAND_SK", INT32),
            new Column("IB_LOWER_BOUND", INT32),
            new Column("IB_UPPER_BOUND", INT32)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::integer, ?::integer);";
        }
    },

    TIME_DIM(
            new long[] {86_400, 86_400, 86_400, 86_400, 86_400, 86_400},
            new Column("T_TIME_SK", INT32),
            new Column("T_TIME_ID", STRING),
            new Column("T_TIME", INT32),
            new Column("T_HOUR", INT32),
            new Column("T_MINUTE", INT32),
            new Column("T_SECOND", INT32),
            new Column("T_AM_PM", STRING),
            new Column("T_SHIFT", STRING),
            new Column("T_SUB_SHIFT", STRING),
            new Column("T_MEAL_TIME", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::varchar(2), ?::varchar(20), ?::varchar(20), ?::varchar(20));";
        }
    },

    CATALOG_RETURNS(
            new long[] {144_067, 143_996_756, 432_018_033, 1_440_033_112, 4_319_925_093L, 14_400_175_879L},
            new Column("CR_RETURNED_DATE_SK", INT32),
            new Column("CR_RETURNED_TIME_SK", INT32),
            new Column("CR_ITEM_SK", INT32),
            new Column("CR_REFUNDED_CUSTOMER_SK", INT32),
            new Column("CR_REFUNDED_CDEMO_SK", INT32),
            new Column("CR_REFUNDED_HDEMO_SK", INT32),
            new Column("CR_REFUNDED_ADDR_SK", INT32),
            new Column("CR_RETURNING_CUSTOMER_SK", INT32),
            new Column("CR_RETURNING_CDEMO_SK", INT32),
            new Column("CR_RETURNING_HDEMO_SK", INT32),
            new Column("CR_RETURNING_ADDR_SK", INT32),
            new Column("CR_CALL_CENTER_SK", INT32),
            new Column("CR_CATALOG_PAGE_SK", INT32),
            new Column("CR_SHIP_MODE_SK", INT32),
            new Column("CR_WAREHOUSE_SK", INT32),
            new Column("CR_REASON_SK", INT32),
            new Column("CR_ORDER_NUMBER", INT32),
            new Column("CR_RETURN_QUANTITY", INT32),
            new Column("CR_RETURN_AMOUNT", DECIMAL),
            new Column("CR_RETURN_TAX", DECIMAL),
            new Column("CR_RETURN_AMT_INC_TAX", DECIMAL),
            new Column("CR_FEE", DECIMAL),
            new Column("CR_RETURN_SHIP_COST", DECIMAL),
            new Column("CR_REFUNDED_CASH", DECIMAL),
            new Column("CR_REVERSED_CHARGE", DECIMAL),
            new Column("CR_STORE_CREDIT", DECIMAL),
            new Column("CR_NET_LOSS", DECIMAL)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2)," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2));";
        }
    },

    REASON(
            new long[] {35, 65, 67, 70, 72, 75},
            new Column("R_REASON_SK", INT32),
            new Column("R_REASON_ID", STRING),
            new Column("R_REASON_DESC", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::varchar(100));";
        }
    },

    STORE(
            new long[] {12, 1_002, 1_350, 1_500, 1_704, 1_902},
            new Column("S_STORE_SK", INT32),
            new Column("S_STORE_ID", STRING),
            new Column("S_REC_START_DATE", DATE),
            new Column("S_REC_END_DATE", DATE),
            new Column("S_CLOSED_DATE_SK", INT32),
            new Column("S_STORE_NAME", STRING),
            new Column("S_NUMBER_EMPLOYEES", INT32),
            new Column("S_FLOOR_SPACE", INT32),
            new Column("S_HOURS", STRING),
            new Column("S_MANAGER", STRING),
            new Column("S_MARKET_ID", INT32),
            new Column("S_GEOGRAPHY_CLASS", STRING),
            new Column("S_MARKET_DESC", STRING),
            new Column("S_MARKET_MANAGER", STRING),
            new Column("S_DIVISION_ID", INT32),
            new Column("S_DIVISION_NAME", STRING),
            new Column("S_COMPANY_ID", INT32),
            new Column("S_COMPANY_NAME", STRING),
            new Column("S_STREET_NUMBER", STRING),
            new Column("S_STREET_NAME", STRING),
            new Column("S_STREET_TYPE", STRING),
            new Column("S_SUITE_NUMBER", STRING),
            new Column("S_CITY", STRING),
            new Column("S_COUNTY", STRING),
            new Column("S_STATE", STRING),
            new Column("S_ZIP", STRING),
            new Column("S_COUNTRY", STRING),
            new Column("S_GMT_OFFSET", DECIMAL),
            new Column("S_TAX_PERCENTAGE", DECIMAL)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::date, ?::date, ?::integer," 
                    + " ?::varchar(50), ?::integer, ?::integer, ?::varchar(20), ?::varchar(40), ?::integer," 
                    + " ?::varchar(100), ?::varchar(100), ?::varchar(40), ?::integer, ?::varchar(50)," 
                    + " ?::integer, ?::varchar(50), ?::varchar(10), ?::varchar(60), ?::varchar(15)," 
                    + " ?::varchar(10), ?::varchar(60), ?::varchar(30), ?::varchar(2), ?::varchar(10)," 
                    + " ?::varchar(20), ?::numeric(5, 2), ?::numeric(5, 2));";
        }
    },

    INVENTORY(
            new long[] {11_745_000, 783_000_000, 1_033_560_000, 1_311_525_000, 1_627_857_000, 1_965_337_830},
            new Column("INV_DATE_SK", INT32),
            new Column("INV_ITEM_SK", INT32),
            new Column("INV_WAREHOUSE_SK", INT32),
            new Column("INV_QUANTITY_ON_HAND", INT32)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::integer, ?::integer, ?::integer);";
        }
    },

    WEB_SALES(
            new long[] {719_384, 720_000_376, 2_159_968_881L, 7_199_963_324L, 21_600_036_511L, 71_999_670_164L},
            new Column("WS_SOLD_DATE_SK", INT32),
            new Column("WS_SOLD_TIME_SK", INT32),
            new Column("WS_SHIP_DATE_SK", INT32),
            new Column("WS_ITEM_SK", INT32),
            new Column("WS_BILL_CUSTOMER_SK", INT32),
            new Column("WS_BILL_CDEMO_SK", INT32),
            new Column("WS_BILL_HDEMO_SK", INT32),
            new Column("WS_BILL_ADDR_SK", INT32),
            new Column("WS_SHIP_CUSTOMER_SK", INT32),
            new Column("WS_SHIP_CDEMO_SK", INT32),
            new Column("WS_SHIP_HDEMO_SK", INT32),
            new Column("WS_SHIP_ADDR_SK", INT32),
            new Column("WS_WEB_PAGE_SK", INT32),
            new Column("WS_WEB_SITE_SK", INT32),
            new Column("WS_SHIP_MODE_SK", INT32),
            new Column("WS_WAREHOUSE_SK", INT32),
            new Column("WS_PROMO_SK", INT32),
            new Column("WS_ORDER_NUMBER", INT32),
            new Column("WS_QUANTITY", INT32),
            new Column("WS_WHOLESALE_COST", DECIMAL),
            new Column("WS_LIST_PRICE", DECIMAL),
            new Column("WS_SALES_PRICE", DECIMAL),
            new Column("WS_EXT_DISCOUNT_AMT", DECIMAL),
            new Column("WS_EXT_SALES_PRICE", DECIMAL),
            new Column("WS_EXT_WHOLESALE_COST", DECIMAL),
            new Column("WS_EXT_LIST_PRICE", DECIMAL),
            new Column("WS_EXT_TAX", DECIMAL),
            new Column("WS_COUPON_AMT", DECIMAL),
            new Column("WS_EXT_SHIP_COST", DECIMAL),
            new Column("WS_NET_PAID", DECIMAL),
            new Column("WS_NET_PAID_INC_TAX", DECIMAL),
            new Column("WS_NET_PAID_INC_SHIP", DECIMAL),
            new Column("WS_NET_PAID_INC_SHIP_TAX", DECIMAL),
            new Column("WS_NET_PROFIT", DECIMAL)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2)," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2)," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2)," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2));";
        }
    },

    DATE_DIM(
            new long[] {73_049, 73_049, 73_049, 73_049, 73_049, 73_049},
            new Column("d_date_sk", INT32),
            new Column("d_date_id", STRING),
            new Column("d_date", DATE),
            new Column("d_month_seq", INT32),
            new Column("d_week_seq", INT32),
            new Column("d_quarter_seq", INT32),
            new Column("d_year", INT32),
            new Column("d_dow", INT32),
            new Column("d_moy", INT32),
            new Column("d_dom", INT32),
            new Column("d_qoy", INT32),
            new Column("d_fy_year", INT32),
            new Column("d_fy_quarter_seq", INT32),
            new Column("d_fy_week_seq", INT32),
            new Column("d_day_name", STRING),
            new Column("d_quarter_name", STRING),
            new Column("d_holiday", STRING),
            new Column("d_weekend", STRING),
            new Column("d_following_holiday", STRING),
            new Column("d_first_dom", INT32),
            new Column("d_last_dom", INT32),
            new Column("d_same_day_ly", INT32),
            new Column("d_same_day_lq", INT32),
            new Column("d_current_day", STRING),
            new Column("d_current_week", STRING),
            new Column("d_current_month", STRING),
            new Column("d_current_quarter", STRING),
            new Column("d_current_year", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::date, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::varchar(9), ?::varchar(6), ?::varchar(1), ?::varchar(1)," 
                    + " ?::varchar(1), ?::integer, ?::integer, ?::integer, ?::integer, ?::varchar(1)," 
                    + " ?::varchar(1), ?::varchar(1), ?::varchar(1), ?::varchar(1));";
        }
    },

    CALL_CENTER(
            new long[] {6, 42, 48, 54, 60, 60},
            new Column("CC_CALL_CENTER_SK", INT32),
            new Column("CC_CALL_CENTER_ID", STRING),
            new Column("CC_REC_START_DATE", DATE),
            new Column("CC_REC_END_DATE", DATE),
            new Column("CC_CLOSED_DATE_SK", INT32),
            new Column("CC_OPEN_DATE_SK", INT32),
            new Column("CC_NAME", STRING),
            new Column("CC_CLASS", STRING),
            new Column("CC_EMPLOYEES", INT32),
            new Column("CC_SQ_FT", INT32),
            new Column("CC_HOURS", STRING),
            new Column("CC_MANAGER", STRING),
            new Column("CC_MKT_ID", INT32),
            new Column("CC_MKT_CLASS", STRING),
            new Column("CC_MKT_DESC", STRING),
            new Column("CC_MARKET_MANAGER", STRING),
            new Column("CC_DIVISION", INT32),
            new Column("CC_DIVISION_NAME", STRING),
            new Column("CC_COMPANY", INT32),
            new Column("CC_COMPANY_NAME", STRING),
            new Column("CC_STREET_NUMBER", STRING),
            new Column("CC_STREET_NAME", STRING),
            new Column("CC_STREET_TYPE", STRING),
            new Column("CC_SUITE_NUMBER", STRING),
            new Column("CC_CITY", STRING),
            new Column("CC_COUNTY", STRING),
            new Column("CC_STATE", STRING),
            new Column("CC_ZIP", STRING),
            new Column("CC_COUNTRY", STRING),
            new Column("CC_GMT_OFFSET", DECIMAL),
            new Column("CC_TAX_PERCENTAGE", DECIMAL)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::date, ?::date, ?::integer," 
                    + " ?::integer, ?::varchar(50), ?::varchar(50), ?::integer, ?::integer, ?::varchar(20)," 
                    + " ?::varchar(40), ?::integer, ?::varchar(50), ?::varchar(100), ?::varchar(40)," 
                    + " ?::integer, ?::varchar(50), ?::integer, ?::varchar(50), ?::varchar(10), ?::varchar(60)," 
                    + " ?::varchar(15), ?::varchar(10), ?::varchar(60), ?::varchar(30), ?::varchar(2)," 
                    + " ?::varchar(10), ?::varchar(20), ?::numeric(5, 2), ?::numeric(5, 2));";
        }
    },

    WEB_RETURNS(
            new long[] {71_763, 71_997_522, 216_003_761, 720_020_485, 2_160_007_345L, 7_199_904_459L},
            new Column("WR_RETURNED_DATE_SK", INT32),
            new Column("WR_RETURNED_TIME_SK", INT32),
            new Column("WR_ITEM_SK", INT32),
            new Column("WR_REFUNDED_CUSTOMER_SK", INT32),
            new Column("WR_REFUNDED_CDEMO_SK", INT32),
            new Column("WR_REFUNDED_HDEMO_SK", INT32),
            new Column("WR_REFUNDED_ADDR_SK", INT32),
            new Column("WR_RETURNING_CUSTOMER_SK", INT32),
            new Column("WR_RETURNING_CDEMO_SK", INT32),
            new Column("WR_RETURNING_HDEMO_SK", INT32),
            new Column("WR_RETURNING_ADDR_SK", INT32),
            new Column("WR_WEB_PAGE_SK", INT32),
            new Column("WR_REASON_SK", INT32),
            new Column("WR_ORDER_NUMBER", INT32),
            new Column("WR_RETURN_QUANTITY", INT32),
            new Column("WR_RETURN_AMT", DECIMAL),
            new Column("WR_RETURN_TAX", DECIMAL),
            new Column("WR_RETURN_AMT_INC_TAX", DECIMAL),
            new Column("WR_FEE", DECIMAL),
            new Column("WR_RETURN_SHIP_COST", DECIMAL),
            new Column("WR_REFUNDED_CASH", DECIMAL),
            new Column("WR_REVERSED_CHARGE", DECIMAL),
            new Column("WR_ACCOUNT_CREDIT", DECIMAL),
            new Column("WR_NET_LOSS", DECIMAL)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::integer, ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2)," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2)," 
                    + " ?::numeric(7, 2));";
        }
    },

    PROMOTION(
            new long[] {300, 1_500, 1_800, 2_000, 2_300, 2_500},
            new Column("P_PROMO_SK", INT32),
            new Column("P_PROMO_ID", STRING),
            new Column("P_START_DATE_SK", INT32),
            new Column("P_END_DATE_SK", INT32),
            new Column("P_ITEM_SK", INT32),
            new Column("P_COST", DECIMAL),
            new Column("P_RESPONSE_TARGET", INT32),
            new Column("P_PROMO_NAME", STRING),
            new Column("P_CHANNEL_DMAIL", STRING),
            new Column("P_CHANNEL_EMAIL", STRING),
            new Column("P_CHANNEL_CATALOG", STRING),
            new Column("P_CHANNEL_TV", STRING),
            new Column("P_CHANNEL_RADIO", STRING),
            new Column("P_CHANNEL_PRESS", STRING),
            new Column("P_CHANNEL_EVENT", STRING),
            new Column("P_CHANNEL_DEMO", STRING),
            new Column("P_CHANNEL_DETAILS", STRING),
            new Column("P_PURPOSE", STRING),
            new Column("P_DISCOUNT_ACTIVE", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::integer, ?::integer," 
                    + " ?::integer, ?::numeric(15, 2), ?::integer, ?::varchar(50), ?::varchar, ?::varchar," 
                    + " ?::varchar, ?::varchar, ?::varchar, ?::varchar, ?::varchar, ?::varchar, ?::varchar(100)," 
                    + " ?::varchar(15), ?::varchar);";
        }
    },

    CUSTOMER_ADDRESS(
            new long[] {50_000, 6_000_000, 15_000_000, 32_500_000, 40_000_000, 50_000_000},
            new Column("CA_ADDRESS_SK", INT32),
            new Column("CA_ADDRESS_ID", STRING),
            new Column("CA_STREET_NUMBER", STRING),
            new Column("CA_STREET_NAME", STRING),
            new Column("CA_STREET_TYPE", STRING),
            new Column("CA_SUITE_NUMBER", STRING),
            new Column("CA_CITY", STRING),
            new Column("CA_COUNTY", STRING),
            new Column("CA_STATE", STRING),
            new Column("CA_ZIP", STRING),
            new Column("CA_COUNTRY", STRING),
            new Column("CA_GMT_OFFSET", DECIMAL),
            new Column("CA_LOCATION_TYPE", STRING)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::varchar(16), ?::varchar(10)," 
                    + " ?::varchar(60), ?::varchar(15), ?::varchar(10), ?::varchar(60), ?::varchar(30)," 
                    + " ?::varchar(2), ?::varchar(10), ?::varchar(20), ?::numeric(5, 2), ?::varchar(20));";
        }
    },

    CATALOG_SALES(
            new long[] {1_441_548, 1_439_980_416, 4_320_078_880L, 14_399_964_710L, 43_200_404_822L, 143_999_334_399L},
            new Column("CS_SOLD_DATE_SK", INT32),
            new Column("CS_SOLD_TIME_SK", INT32),
            new Column("CS_SHIP_DATE_SK", INT32),
            new Column("CS_BILL_CUSTOMER_SK", INT32),
            new Column("CS_BILL_CDEMO_SK", INT32),
            new Column("CS_BILL_HDEMO_SK", INT32),
            new Column("CS_BILL_ADDR_SK", INT32),
            new Column("CS_SHIP_CUSTOMER_SK", INT32),
            new Column("CS_SHIP_CDEMO_SK", INT32),
            new Column("CS_SHIP_HDEMO_SK", INT32),
            new Column("CS_SHIP_ADDR_SK", INT32),
            new Column("CS_CALL_CENTER_SK", INT32),
            new Column("CS_CATALOG_PAGE_SK", INT32),
            new Column("CS_SHIP_MODE_SK", INT32),
            new Column("CS_WAREHOUSE_SK", INT32),
            new Column("CS_ITEM_SK", INT32),
            new Column("CS_PROMO_SK", INT32),
            new Column("CS_ORDER_NUMBER", INT32),
            new Column("CS_QUANTITY", INT32),
            new Column("CS_WHOLESALE_COST", DECIMAL),
            new Column("CS_LIST_PRICE", DECIMAL),
            new Column("CS_SALES_PRICE", DECIMAL),
            new Column("CS_EXT_DISCOUNT_AMT", DECIMAL),
            new Column("CS_EXT_SALES_PRICE", DECIMAL),
            new Column("CS_EXT_WHOLESALE_COST", DECIMAL),
            new Column("CS_EXT_LIST_PRICE", DECIMAL),
            new Column("CS_EXT_TAX", DECIMAL),
            new Column("CS_COUPON_AMT", DECIMAL),
            new Column("CS_EXT_SHIP_COST", DECIMAL),
            new Column("CS_NET_PAID", DECIMAL),
            new Column("CS_NET_PAID_INC_TAX", DECIMAL),
            new Column("CS_NET_PAID_INC_SHIP", DECIMAL),
            new Column("CS_NET_PAID_INC_SHIP_TAX", DECIMAL),
            new Column("CS_NET_PROFIT", DECIMAL)
    ) {
        @Override
        public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES (?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer, ?::integer," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2)," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2)," 
                    + " ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2), ?::numeric(7, 2));";
        }
    };

    private final Column[] columns;
    private final long[] tableSizes;

    TpcdsTables(long[] tableSizes, Column... columns) {
        this.tableSizes = tableSizes;
        this.columns = columns;
    }

    /** Returns name of the table. */
    @Override
    public String tableName() {
        return name().toLowerCase();
    }

    /** Returns number of column in the table. */
    @Override
    public int columnsCount() {
        return columns.length;
    }

    @Override
    public String columnName(int idx) {
        return columns[idx].name;
    }

    /** Returns definition of a table including necessary indexes. */
    @Override
    public String ddlScript() {
        return TpchHelper.loadFromResource("tpcds/ddl/" + tableName() + ".sql");
    }

    @SuppressWarnings("resource")
    @Override
    public Iterator<Object[]> dataProvider(Path pathToDataset) throws IOException {
        return Files.lines(pathToDataset.resolve(tableName() + ".dat"))
                .map(this::csvLineToTableValues)
                .iterator();
    }

    @Override
    public long estimatedSize(TpcScaleFactor sf) {
        return sf.size(tableSizes);
    }

    private Object[] csvLineToTableValues(String line) {
        String[] stringValues = line.split("\\|");
        Object[] values = new Object[columns.length];

        for (int i = 0; i < columns.length; i++) {
            if (stringValues.length <= i) {
                // all trailing columns are nulls
                continue;
            }

            String value = stringValues[i];
            if (value.isEmpty()) {
                continue;
            }

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
