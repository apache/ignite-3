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

package org.apache.ignite.internal.lang;

import java.util.Collection;
import org.jetbrains.annotations.TestOnly;

/**
 * Interface provides method for dumping object state.
 */
public interface Debuggable {
    /**
     * Indentation string used for formatting child components.
     */
    String INDENTATION = "  ";

    /**
     * Returns the indentation string for child components.
     *
     * @param indent Current indentation string.
     */
    static String childIndentation(String indent) {
        return indent + INDENTATION;
    }

    /**
     * Dumps the state of this component to the provided writer. This method is used for debugging purposes only.
     *
     * @param writer Writer to which output should be written.
     * @param indent Current indentation string for formatting child components.
     */
    @TestOnly
    default void dumpState(IgniteStringBuilder writer, String indent) {
        // No-op.
    }

    /**
     * Dumps the state of provided components to the provided writer. This method is used for debugging purposes only.
     *
     * @param writer Writer to which output should be written.
     * @param indent Current indentation string for formatting components.
     * @param components Collection of components to be dumped.
     */
    @TestOnly
    static void dumpState(IgniteStringBuilder writer, String indent, Collection<? extends Debuggable> components) {
        if (components.isEmpty()) {
            return;
        }

        for (Debuggable child : components) {
            child.dumpState(writer, indent);
        }
    }
}
