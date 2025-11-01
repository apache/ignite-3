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

package org.apache.ignite.internal.jdbc2;

import java.sql.SQLException;

final class ResultSetWrapper {

    private JdbcResultSet resultSet;

    ResultSetWrapper(JdbcResultSet first) {
        resultSet = first;
    }

    void setCloseStatement(boolean b) {
        resultSet.setCloseStatement(b);
    }

    JdbcResultSet current() {
        return resultSet;
    }

    boolean isQuery() {
        ClientSyncResultSet rs = resultSet.resultSet();

        return rs.hasRowSet();
    }

    int updateCount() {
        JdbcResultSet rs = resultSet;

        ClientSyncResultSet clientResultSet = rs.resultSet();
        if (clientResultSet.hasRowSet()) {
            return -1;
        } else if (clientResultSet.affectedRows() == -1) {
            // DDL or control statements
            return 0;
        } else {
            return (int) clientResultSet.affectedRows();
        }
    }

    boolean nextResultSet() throws SQLException {
        JdbcResultSet current = resultSet;
        if (current == null) {
            return false;
        }

        try {
            JdbcResultSet nextRs = current.tryNextResultSet();
            boolean hasNext = nextRs != null;

            resultSet = nextRs;

            return hasNext;
        } finally {
            current.close();
        }
    }

    void close() throws SQLException {
        JdbcResultSet current = resultSet;
        if (current == null || current.closed) {
            return;
        }
        resultSet = null;

        JdbcResultSet rs = current;

        do {
            // w/o iteration over all cursors, the cursors that are
            // not retrieved hung in the void and are never released.
            try {
                rs = rs.tryNextResultSet();
            } catch (SQLException ignore) {
                // This is an execution related exception, ignore it. 
                break;
            }
        } while (rs != null);

        current.close();
    }
}
