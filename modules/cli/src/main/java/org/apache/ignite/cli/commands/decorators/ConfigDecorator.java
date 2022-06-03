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

package org.apache.ignite.cli.commands.decorators;

import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;
import org.apache.ignite.cli.config.Config;

/**
 * Decorator for printing {@link Config}.
 */
public class ConfigDecorator implements Decorator<Config, TerminalOutput> {
    @Override
    public TerminalOutput decorate(Config data) {
        StringBuilder builder = new StringBuilder();
        for (Iterator<Entry<Object, Object>> iterator = data.getProperties().entrySet().iterator(); iterator.hasNext(); ) {
            Entry<Object, Object> entry = iterator.next();
            builder.append(entry.getKey()).append("=").append(entry.getValue());
            if (iterator.hasNext()) {
                builder.append(System.lineSeparator());
            }
        }
        return builder::toString;
    }
}
