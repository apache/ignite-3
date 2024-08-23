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

package org.apache.ignite.internal.sql.sqllogic;

import java.util.Arrays;

/**
 * EndLoop command serves as a marker that ends a block of commands that forms a loop.
 *
 * @see For loop command
 */
final class EndFor extends Command {

    /** Creates an instance of Endloop command by parsing it from script and token. **/
    EndFor(Script script, ScriptContext ctx, String[] tokens) {
        super(script.scriptPosition());
        if (tokens.length > 1) {
            throw new IllegalArgumentException("EndLoop accepts no arguments: " + Arrays.toString(tokens));
        }
    }

    @Override
    void execute(ScriptContext ctx) {
        // No-op.
    }
}
