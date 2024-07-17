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

package org.apache.ignite.internal.sql.engine.exec.mapping;

/**
 * Represents a target to execute fragment on.
 *
 * <p>Depending on type of the target, it may contain list of required nodes,
 * list of optional node with single or multiple choice, or list of assignments.
 *
 * <p>To resolve actual list of nodes and assignments, use corresponding methods of
 * {@link ExecutionTargetFactory factory} this target was created by.
 *
 * @see ExecutionTargetFactory
 */
public interface ExecutionTarget {
    /**
     * Colocates this target with given one.
     *
     * <p>Colocation is a process of finding intersection of the given two targets. For example,
     * lets assume that we have two targets T1 and T2. T1 may be execute on one of the nodes
     * [N1, N2, N3]. T2 may be executed on one of the nodes [N2, N3, N4, N5]. The result of
     * colocation of T1 and T2 will be target OneOf[N2, N3].
     *
     * @param other A target to colocate with.
     * @return A colocated target.
     * @throws ColocationMappingException In case these targets can't be colocated.
     */
    ExecutionTarget colocateWith(ExecutionTarget other) throws ColocationMappingException;

    /**
     * Removes options from current target which are not colocated with other target.
     *
     * <p>If target has several options, remove those are not presented in given target to improve colocation.
     *
     * @param other Target with which we need to colocate current target.
     * @return Returns new target in case current has been adjusted, return {@code this} instance otherwise.
     */
    ExecutionTarget trimTo(ExecutionTarget other);
}
