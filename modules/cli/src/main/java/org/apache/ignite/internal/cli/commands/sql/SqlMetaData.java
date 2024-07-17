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

package org.apache.ignite.internal.cli.commands.sql;

/**
 * Constants for SQL auto-completion.
 */

class SqlMetaData {

    /**
     * SQL reserved keywords.
     */
    static final String[] KEYWORDS = { // TODO: https://issues.apache.org/jira/browse/IGNITE-16973
            "ABSOLUTE", "ACTION", "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "AS", "ASC", "ASSERTION", "AT", "AUTHORIZATION",
            "AVG", "BEGIN", "BETWEEN", "BIT", "BIT_LENGTH", "BOTH", "BY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CHAR",
            "CHARACTER", "CHAR_LENGTH", "CHARACTER_LENGTH", "CHECK", "CLOSE", "COALESCE", "COLLATE", "COLLATION", "COLUMN", "COMMIT",
            "CONNECT", "CONNECTION", "CONSTRAINT", "CONSTRAINTS", "CONTINUE", "CONVERT", "CORRESPONDING", "COUNT", "CREATE", "CROSS",
            "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATE", "DAY", "DEALLOCATE", "DEC",
            "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DESCRIBE", "DESCRIPTOR", "DIAGNOSTICS",
            "DISCONNECT", "DISTINCT", "DOMAIN", "DOUBLE", "DROP", "ELSE", "END", "END-EXEC", "ESCAPE", "EXCEPT", "EXCEPTION", "EXEC",
            "EXECUTE", "EXISTS", "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FIRST", "FLOAT", "FOR", "FOREIGN", "FOUND", "FROM", "FULL",
            "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GROUP", "HAVING", "HOUR", "IDENTITY", "IMMEDIATE", "IN", "INDICATOR", "INITIALLY",
            "INNER", "INPUT", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION", "JOIN", "KEY",
            "LANGUAGE", "LAST", "LEADING", "LEFT", "LEVEL", "LIKE", "LOCAL", "LOWER", "MATCH", "MAX", "MIN", "MINUTE", "MODULE", "MONTH",
            "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NEXT", "NO", "NOT", "NULL", "NULLIF", "NUMERIC", "OCTET_LENGTH", "OF", "ON", "ONLY",
            "OPEN", "OPTION", "OR", "ORDER", "OUTER", "OUTPUT", "OVERLAPS", "PAD", "PARTIAL", "POSITION", "PRECISION", "PREPARE",
            "PRESERVE", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE", "PUBLIC", "READ", "REAL", "REFERENCES", "RELATIVE", "RESTRICT",
            "REVOKE", "RIGHT", "ROLLBACK", "ROWS", "SCHEMA", "SCROLL", "SECOND", "SECTION", "SELECT", "SESSION", "SESSION_USER", "SET",
            "SIZE", "SMALLINT", "SOME", "SPACE", "SQL", "SQLCODE", "SQLERROR", "SQLSTATE", "SUBSTRING", "SUM", "SYSTEM_USER", "TABLE",
            "TEMPORARY", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSACTION", "TRANSLATE",
            "TRANSLATION", "TRIM", "TRUE", "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPPER", "USAGE", "USER", "USING", "VALUE", "VALUES",
            "VARCHAR", "VARYING", "VIEW", "WHEN", "WHENEVER", "WHERE", "WITH", "WORK", "WRITE", "YEAR", "ZONE", "ADA", "C", "CATALOG_NAME",
            "CHARACTER_SET_CATALOG", "CHARACTER_SET_NAME", "CHARACTER_SET_SCHEMA", "CLASS_ORIGIN", "COBOL", "COLLATION_CATALOG",
            "COLLATION_NAME", "COLLATION_SCHEMA", "COLUMN_NAME", "COMMAND_FUNCTION", "COMMITTED", "CONDITION_NUMBER", "CONNECTION_NAME",
            "CONSTRAINT_CATALOG", "CONSTRAINT_NAME", "CONSTRAINT_SCHEMA", "CURSOR_NAME", "DATA", "DATETIME_INTERVAL_CODE",
            "DATETIME_INTERVAL_PRECISION", "DYNAMIC_FUNCTION", "FORTRAN", "LENGTH", "MESSAGE_LENGTH", "MESSAGE_OCTET_LENGTH",
            "MESSAGE_TEXT", "MORE", "MUMPS", "NAME", "NULLABLE", "NUMBER", "PASCAL", "PLI", "REPEATABLE", "RETURNED_LENGTH",
            "RETURNED_OCTET_LENGTH", "RETURNED_SQLSTATE", "ROW_COUNT", "SCALE", "SCHEMA_NAME", "SERIALIZABLE", "SERVER_NAME",
            "SUBCLASS_ORIGIN", "TABLE_NAME", "TYPE", "UNCOMMITTED", "UNNAMED"
    };

    /**
     * List of keywords which must be at the beginning of the statement, used for very basic completion.
     */
    static final String[] STARTING_KEYWORDS = {
            "ALTER", "SET", "CREATE", "DROP", "WITH", "SELECT", "EXPLAIN", "DESCRIBE", "INSERT", "DELETE", "UPDATE", "MERGE", "CALL"
    };

    // These lists are copied from calcite
    /**
     * SQL numeric functions.
     */
    static final String[] NUMERIC_FUNCTIONS = {
            "ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CBRT", "CEILING", "COS", "COT", "DEGREES", "EXP", "FLOOR", "LOG", "LOG10", "MOD", "PI",
            "POWER", "RADIANS", "RAND", "ROUND", "SIGN", "SIN", "SQRT", "TAN", "TRUNCATE"
    };

    /**
     * SQL string functions.
     */
    static final String[] STRING_FUNCTIONS = {
            "ASCII", "CHAR", "CONCAT", "DIFFERENCE", "INSERT", "LCASE", "LEFT", "LENGTH", "LOCATE",
            "LTRIM", "REPEAT", "REPLACE", "RIGHT", "RTRIM", "SOUNDEX", "SPACE", "SUBSTRING", "UCASE"
    };

    /**
     * SQL date/time functions.
     */
    static final String[] TIME_DATE_FUNCTIONS = {
            "CONVERT_TIMEZONE", "CURDATE", "CURTIME", "DAYNAME", "DAYOFMONTH", "DAYOFWEEK", "DAYOFYEAR", "HOUR", "MINUTE", "MONTH",
            "MONTHNAME", "NOW", "QUARTER", "SECOND", "TIMESTAMPADD", "TIMESTAMPDIFF", "TO_DATE", "TO_TIMESTAMP", "WEEK", "YEAR"
    };

    /**
     * SQL system functions.
     */
    static final String[] SYSTEM_FUNCTIONS = {
            "CONVERT", "DATABASE", "IFNULL", "USER"
    };
}
