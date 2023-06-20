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

package org.apache.ignite.internal.deployunit.metastore;

import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;

/**
 * Listener of deployment unit cluster status changes.
 */
public abstract class ClusterEventCallback {
    /**
     * Change event.
     *
     * @param status Deployment unit status.
     */
    public void onUpdate(UnitClusterStatus status) {
        switch (status.status()) {
            case UPLOADING:
                onUploading(status);
                break;
            case DEPLOYED:
                onDeploy(status);
                break;
            case OBSOLETE:
                onObsolete(status);
                break;
            case REMOVING:
                onRemoving(status);
                break;
            default:
                break;
        }
    }

    protected void onUploading(UnitClusterStatus status) {

    }

    protected void onDeploy(UnitClusterStatus status) {

    }

    protected void onObsolete(UnitClusterStatus status) {

    }

    protected void onRemoving(UnitClusterStatus status) {

    }
}
