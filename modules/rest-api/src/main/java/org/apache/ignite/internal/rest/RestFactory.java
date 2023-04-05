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

package org.apache.ignite.internal.rest;

import io.micronaut.runtime.Micronaut;

/**
 * Factory that produces all beans that is necessary for the controller class.
 */
public interface RestFactory {
    /**
     * Destroy method. All resources of the factory implementation must be cleaned and all fields must be set to {@code null}.
     *      The reason of these requirements is Micronaut design.
     *      {@link Micronaut#start()} store shutdown hook and capture a pointer
     *      to the embedded application {@link io.micronaut.http.server.netty.NettyEmbeddedServer} and as a result
     *      {@link io.micronaut.context.ApplicationContext} will never be collected by the GC.
     *      All rest factories stored in the application context should be cleaned to prevent memory leak.
     */
    void cleanResources();
}
