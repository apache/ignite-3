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

package org.apache.ignite.internal.compute;

import org.apache.ignite.internal.compute.message.DeploymentUnitMsg;
import org.apache.ignite.internal.compute.message.ExecuteRequest;
import org.apache.ignite.internal.compute.message.ExecuteRequestV2;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.internal.compute.message.JobCancelRequest;
import org.apache.ignite.internal.compute.message.JobCancelResponse;
import org.apache.ignite.internal.compute.message.JobChangePriorityRequest;
import org.apache.ignite.internal.compute.message.JobChangePriorityResponse;
import org.apache.ignite.internal.compute.message.JobResultRequest;
import org.apache.ignite.internal.compute.message.JobResultResponse;
import org.apache.ignite.internal.compute.message.JobStateRequest;
import org.apache.ignite.internal.compute.message.JobStateResponse;
import org.apache.ignite.internal.compute.message.JobStatesRequest;
import org.apache.ignite.internal.compute.message.JobStatesResponse;
import org.apache.ignite.internal.network.annotations.MessageGroup;

/**
 * Message types for the Compute module.
 */
@MessageGroup(groupType = 6, groupName = "ComputeMessages")
public class ComputeMessageTypes {
    /**
     * Type for {@link ExecuteRequest}.
     */
    public static final short EXECUTE_REQUEST = 0;

    /**
     * Type for {@link ExecuteResponse}.
     */
    public static final short EXECUTE_RESPONSE = 1;

    /**
     * Type for {@link DeploymentUnitMsg}.
     */
    public static final short DEPLOYMENT_UNIT = 2;

    /** Type for {@link JobResultRequest}. */
    public static final short JOB_RESULT_REQUEST = 3;

    /** Type for {@link JobResultResponse}. */
    public static final short JOB_RESULT_RESPONSE = 4;

    /** Type for {@link JobStateRequest}. */
    public static final short JOB_STATE_REQUEST = 5;

    /** Type for {@link JobStateResponse}. */
    public static final short JOB_STATE_RESPONSE = 6;

    /** Type for {@link JobCancelRequest}. */
    public static final short JOB_CANCEL_REQUEST = 7;

    /** Type for {@link JobCancelResponse}. */
    public static final short JOB_CANCEL_RESPONSE = 8;

    /** Type for {@link JobChangePriorityRequest}. */
    public static final short JOB_CHANGE_PRIORITY_REQUEST = 9;

    /** Type for {@link JobChangePriorityResponse}. */
    public static final short JOB_CHANGE_PRIORITY_RESPONSE = 10;

    /** Type for {@link JobStatesRequest}. */
    public static final short JOB_STATES_REQUEST = 11;

    /** Type for {@link JobStatesResponse}. */
    public static final short JOB_STATES_RESPONSE = 12;

    /** Type for {@link ExecuteRequestV2}. */
    public static final short EXECUTE_REQUEST_V2 = 13;

}
