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

package org.apache.ignite.internal.systemview;

import java.util.List;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.schema.row.InternalTuple;

/**
 * The system view manager is responsible for registering system views in the cluster.
 */
public interface SystemViewManager extends IgniteComponent {
    /**
     * Registers a system view.
     *
     * <p>Registration of views is completed when the system view manager starts. Therefore,
     * it is necessary for other components to register the views before the manager is started.
     *
     * @param view System view to register.
     */
    void register(SystemView<?> view);

    /**
     * Returns a list of nodes a view with given name can be found on.
     *
     * @param name Name of view of interest.
     * @return A list of nodes owning a view, or empty list if there is no node in the cluster owning this view.
     */
    List<String> owningNodes(String name);

    /**
     * Opens a cursor over view with a given name and returns publisher emitting rows of the view.
     *
     * @param name Name of view of interest.
     * @return A publisher.
     * @throws IgniteInternalException if view with given name is not presented on local node.
     */
    Publisher<InternalTuple> scanView(String name) throws IgniteInternalException;
}
