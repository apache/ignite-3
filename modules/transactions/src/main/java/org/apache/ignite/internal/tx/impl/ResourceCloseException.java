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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Common.RESOURCE_CLOSING_ERR;

import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalException;

/**
 * The exception is thrown when a resource fails to close.
 */
public class ResourceCloseException extends IgniteInternalException {

    private static final long serialVersionUID = 3883849202706660395L;

    private final FullyQualifiedResourceId resourceId;

    public ResourceCloseException(
            FullyQualifiedResourceId resourceId,
            UUID remoteHostId,
            Throwable cause
    ) {
        super(RESOURCE_CLOSING_ERR, format("Resource close exception [resourceId={}, remoteHostId={}]", resourceId, remoteHostId), cause);
        this.resourceId = resourceId;
    }

    public FullyQualifiedResourceId resourceId() {
        return resourceId;
    }
}
