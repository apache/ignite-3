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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.sql.engine.util.Commons.checkRange;

import java.io.Serializable;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * This class represents the volatile state that may be propagated from parent to its children
 * during rewind.
 */
public class SharedState implements Serializable {
    private static final long serialVersionUID = 42L;

    private Object[] correlations = new Object[16];

    /**
     * Gets correlated value.
     *
     * @param id Correlation ID.
     * @return Correlated value.
     */
    public Object correlatedVariable(int id) {
        checkRange(correlations, id);

        return correlations[id];
    }

    /**
     * Sets correlated value.
     *
     * @param id Correlation ID.
     * @param value Correlated value.
     */
    public void correlatedVariable(int id, Object value) {
        correlations = Commons.ensureCapacity(correlations, id + 1);

        correlations[id] = value;
    }
}
