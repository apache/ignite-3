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

package org.apache.ignite.internal.sql.engine.exec.mapping.smallcluster;

import java.util.List;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationMappingException;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTarget;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetFactory;

/**
 * Represents a target that should be executed on all of the given nodes.
 *
 * <p>See javadoc of {@link ExecutionTargetFactory#allOf(List)} for details.
 */
class AllOfTarget extends AbstractTarget {
    AllOfTarget(long nodes) {
        super(nodes);
    }

    @Override
    boolean finalised() {
        return true;
    }

    @Override
    public ExecutionTarget finalise() {
        return this;
    }

    @Override
    public ExecutionTarget colocateWith(ExecutionTarget other) throws ColocationMappingException {
        assert other instanceof AbstractTarget : other == null ? "<null>" : other.getClass().getCanonicalName();

        return ((AbstractTarget) other).colocate(this);
    }

    @Override
    public ExecutionTarget trimTo(ExecutionTarget other) {
        return this;
    }

    @Override
    ExecutionTarget colocate(AllOfTarget other) throws ColocationMappingException {
        return colocate(this, other);
    }

    @Override
    ExecutionTarget colocate(OneOfTarget other) throws ColocationMappingException {
        return colocate(this, other);
    }

    @Override
    ExecutionTarget colocate(PartitionedTarget other) throws ColocationMappingException {
        return colocate(this, other);
    }

    @Override
    ExecutionTarget colocate(SomeOfTarget other) throws ColocationMappingException {
        return colocate(this, other);
    }
}
