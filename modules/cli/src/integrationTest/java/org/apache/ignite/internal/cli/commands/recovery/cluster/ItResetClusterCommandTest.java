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

package org.apache.ignite.internal.cli.commands.recovery.cluster;

import org.apache.ignite.internal.cli.commands.recovery.cluster.reset.ResetClusterCommand;
import org.apache.ignite.internal.util.ArrayUtils;

/** Test class for {@link ResetClusterCommand}. */
class ItResetClusterCommandTest extends ItResetClusterTest {
    @Override
    protected void execute(String... args) {
        String[] fullArgs = ArrayUtils.concat(new String[] {"recovery", "cluster", "reset"}, args);

        super.execute(fullArgs);
    }
}
