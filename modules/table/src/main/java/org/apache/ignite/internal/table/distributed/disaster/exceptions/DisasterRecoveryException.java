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

package org.apache.ignite.internal.table.distributed.disaster.exceptions;

import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.lang.ErrorGroups.DisasterRecovery;
import org.intellij.lang.annotations.MagicConstant;

/** Common exception for disaster recovery. */
public class DisasterRecoveryException extends IgniteInternalException {
    private static final long serialVersionUID = -3565357739782565015L;

    public DisasterRecoveryException(@MagicConstant(valuesFromClass = DisasterRecovery.class) int code, Throwable cause) {
        super(code, cause);
    }

    public DisasterRecoveryException(@MagicConstant(valuesFromClass = DisasterRecovery.class) int code, String message) {
        super(code, message);
    }
}
