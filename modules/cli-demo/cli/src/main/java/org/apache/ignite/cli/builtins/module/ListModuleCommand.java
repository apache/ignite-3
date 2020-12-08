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

package org.apache.ignite.cli.builtins.module;

import java.util.Arrays;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;
import org.apache.ignite.cli.AbstractCliCommand;

@Singleton
public class ListModuleCommand extends AbstractCliCommand {

    public void list() {
        var builtinModules = ModuleManager.readBuiltinModules()
            .stream()
            .filter(m -> !m.name.startsWith(ModuleManager.INTERNAL_MODULE_PREFIX));
        String table = AsciiTable.getTable(builtinModules.collect(Collectors.toList()), Arrays.asList(
            new Column().header("Name").dataAlign(HorizontalAlign.LEFT).with(m -> m.name),
            new Column().header("Description").dataAlign(HorizontalAlign.LEFT).with(m -> m.description)
        ));
        out.println(table);
    }
}
