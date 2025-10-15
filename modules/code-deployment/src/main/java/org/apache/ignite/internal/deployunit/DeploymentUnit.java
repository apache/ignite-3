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

package org.apache.ignite.internal.deployunit;

import java.util.concurrent.CompletableFuture;

/**
 * Interface representing a deployment unit in the Apache Ignite code deployment system.
 * 
 * <p>A deployment unit is a container for code and resources that can be deployed to and managed 
 * within an Ignite cluster. This interface provides a contract for processing deployment unit 
 * content through a processor pattern, allowing for flexible handling of different types of 
 * deployment units such as regular file-based units and ZIP-compressed units.
 */
public interface DeploymentUnit extends AutoCloseable {
    /**
     * Processes the deployment unit content using the provided processor.
     * 
     * <p>This method delegates the processing of the deployment unit to the specified processor,
     * following a strategy pattern. The processor determines how the unit content should be
     * handled based on its implementation. Different processors can perform different operations
     * on the same deployment unit.
     * 
     * <p>The method supports generic type parameters to allow processors to work with different
     * argument types and return different result types, providing flexibility for various
     * processing scenarios.
     * 
     * <p>For ZIP-based deployment units, the processor may need to handle both regular content
     * and compressed content that requires extraction.
     *
     *
     * @param <T> the type of argument passed to the processor
     * @param <R> the type of result
     * @param processor the processor that will handle the deployment unit content
     * @param arg the argument to be passed to the processor during processing
     */
    <T, R> CompletableFuture<R> process(DeploymentUnitProcessor<T, R> processor, T arg);
}
