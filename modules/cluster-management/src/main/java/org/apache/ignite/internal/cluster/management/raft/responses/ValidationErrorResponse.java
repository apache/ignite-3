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

package org.apache.ignite.internal.cluster.management.raft.responses;

import java.io.Serializable;

/**
 * Response that indicates that a join request has been rejected.
 */
public class ValidationErrorResponse implements Serializable {
    private final String reason;

    /**
     * Creates a new response.
     *
     * @param reason Textual representation of the reason of join rejection.
     */
    public ValidationErrorResponse(String reason) {
        this.reason = reason;
    }

    /**
     * Returns the textual representation of the reason of join rejection.
     *
     * @return Textual representation of the reason of join rejection.
     */
    public String reason() {
        return reason;
    }
}
