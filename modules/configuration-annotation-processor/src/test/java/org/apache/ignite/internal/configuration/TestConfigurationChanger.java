/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.configuration;

import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.tree.InnerNode;

import static java.util.concurrent.CompletableFuture.completedFuture;

/** Implementation of {@link ConfigurationChanger} to be used in tests. Has no support of listeners. */
public class TestConfigurationChanger extends ConfigurationChanger {
    /** Runtime implementations generator for node classes. */
    private final ConfigurationAsmGenerator cgen;

    /**
     * @param cgen Runtime implementations generator for node classes. Will be used to instantiate nodes objects.
     */
    public TestConfigurationChanger(ConfigurationAsmGenerator cgen) {
        super((oldRoot, newRoot, revision) -> completedFuture(null));

        this.cgen = cgen;
    }

    /** {@inheritDoc} */
    @Override public void addRootKey(RootKey<?, ?> rootKey) {
        super.addRootKey(rootKey);

        cgen.compileRootSchema(rootKey.schemaClass());
    }

    /** {@inheritDoc} */
    @Override public InnerNode createRootNode(RootKey<?, ?> rootKey) {
        return cgen.instantiateNode(rootKey.schemaClass());
    }
}
