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

package org.apache.ignite.internal.table.distributed;

import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.lang.ErrorGroups.MetaStorage;

/**
 * Exception for a failure in case of writing an assignments list for a table to a metastore.
 */
public class WritingAssignmentsToMetastoreException extends IgniteInternalException {

    private static final long serialVersionUID = 8286055009369769180L;

    private final int tableId;

    private final List<Assignments> assignments;

    private final boolean isAssignmentsWereAvailable;

    /**
     * Full constructor for the exception.
     *
     * @param tableId the identifier of a table which for assignments list tried to be written.
     * @param assignments generated assignments list to be written into a metastore.
     * @param cause any throwable cause that was the reason for metastore writing failure.
     */
    public WritingAssignmentsToMetastoreException(int tableId, List<Assignments> assignments, Throwable cause) {
        super(
                detectCorrectOpErrorCode(cause),
                "Failure while writing assignments " + Assignments.assignmentListToString(assignments)
                        + " for table "  + tableId + " after invoke completion",
                cause);
        this.tableId = tableId;
        this.assignments = assignments;
        isAssignmentsWereAvailable = true;
    }

    /**
     * Constructor for the exception if assignments list is unavailable
     *
     * @param tableId the identifier of a table which for assignments list tried to be written.
     * @param cause any throwable cause that was the reason for metastore writing failure.
     */
    public WritingAssignmentsToMetastoreException(int tableId, Throwable cause) {
        super(
                detectCorrectOpErrorCode(cause),
                "Failure while writing assignments for table "  + tableId + " before or during metastore invoke",
                cause);
        this.tableId = tableId;
        this.assignments = null;
        isAssignmentsWereAvailable = false;
    }

    /**
     * If the reason was a timeout should pass the corresponding OP error code.
     *
     * @param cause given failure reason.
     * @return OP_EXECUTION_TIMEOUT_ERR if the cause was a timeout or OP_EXECUTION_ERR otherwise.
     */
    private static int detectCorrectOpErrorCode(Throwable cause) {
        return cause instanceof TimeoutException
                ? MetaStorage.OP_EXECUTION_TIMEOUT_ERR
                : MetaStorage.OP_EXECUTION_ERR;
    }

    /**
     * Identifier of a table which for assignments were generated.
     *
     * @return Identifier of a table which for assignments were generated.
     * */
    public int getTableId() {
        return tableId;
    }

    /**
     * Generated assignments list that couldn't be written to a metastore. If returns null-value, then should be checked that
     * {@see WritingAssignmentsToMetastoreException#isAssignmentsWereAvailable} isn't false to be sure that assignments were actually
     * provided in the exception.
     *
     * @return Generated assignments list that couldn't be written to a metastore.
     */
    public List<Assignments> getAssignments() {
        return assignments;
    }

    /**
     * Is assignments list was provided in this exception or not.
     *
     * @return true if the list (even null-value) was provided or false otherwise.
     */
    public boolean isAssignmentsWereAvailable() {
        return isAssignmentsWereAvailable;
    }
}
