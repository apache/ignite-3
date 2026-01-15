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

package org.apache.ignite.internal.cli.decorators;

import static org.apache.ignite.internal.util.IgniteUtils.readableSize;

import java.util.List;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.rest.client.model.UnitEntry;
import org.apache.ignite.rest.client.model.UnitFile;
import org.apache.ignite.rest.client.model.UnitFolder;

/** Decorates unit inspect result as a tree. */
public class UnitInspectDecorator implements Decorator<UnitFolder, TerminalOutput> {

    private static final String TREE_BRANCH = "+-- ";
    private static final String TREE_LAST = "\\-- ";
    private static final String TREE_VERTICAL = "|   ";
    private static final String TREE_SPACE = "    ";

    private final boolean plain;

    public UnitInspectDecorator(boolean plain) {
        this.plain = plain;
    }

    @Override
    public TerminalOutput decorate(UnitFolder data) {
        StringBuilder sb = new StringBuilder();

        if (plain) {
            renderPlain(data, sb, "");
        } else {
            sb.append(data.getName()).append('\n');
            renderTree(data.getChildren(), sb, "");
        }

        String result = sb.toString();
        return () -> result;
    }

    private static void renderTree(List<UnitEntry> children, StringBuilder sb, String prefix) {
        if (children == null || children.isEmpty()) {
            return;
        }

        for (int i = 0; i < children.size(); i++) {
            boolean isLast = i == children.size() - 1;
            UnitEntry child = children.get(i);

            Object actualInstance = child.getActualInstance();
            if (actualInstance == null) {
                continue;
            }

            String connector = isLast ? TREE_LAST : TREE_BRANCH;

            if (actualInstance instanceof UnitFile) {
                UnitFile file = (UnitFile) actualInstance;
                sb.append(prefix).append(connector).append(file.getName())
                        .append(" (").append(readableSize(file.getSize(), false)).append(')')
                        .append('\n');
            } else if (actualInstance instanceof UnitFolder) {
                UnitFolder folder = (UnitFolder) actualInstance;
                sb.append(prefix).append(connector).append(folder.getName()).append('\n');
                String newPrefix = prefix + (isLast ? TREE_SPACE : TREE_VERTICAL);
                renderTree(folder.getChildren(), sb, newPrefix);
            }
        }
    }

    private static void renderPlain(UnitFolder folder, StringBuilder sb, String prefix) {
        if (folder == null) {
            return;
        }

        List<UnitEntry> children = folder.getChildren();
        if (children == null || children.isEmpty()) {
            return;
        }

        for (UnitEntry child : children) {
            Object actualInstance = child.getActualInstance();
            if (actualInstance == null) {
                continue;
            }

            if (actualInstance instanceof UnitFile) {
                UnitFile file = (UnitFile) actualInstance;
                sb.append(prefix);
                if (!prefix.isEmpty()) {
                    sb.append('/');
                }
                sb.append(file.getName())
                        .append(' ').append(file.getSize())
                        .append('\n');
            } else if (actualInstance instanceof UnitFolder) {
                UnitFolder subFolder = (UnitFolder) actualInstance;
                String newPrefix = prefix.isEmpty() ? subFolder.getName() : prefix + "/" + subFolder.getName();
                renderPlain(subFolder, sb, newPrefix);
            }
        }
    }

}
