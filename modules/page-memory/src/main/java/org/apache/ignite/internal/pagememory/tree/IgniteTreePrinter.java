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

package org.apache.ignite.internal.pagememory.tree;

import java.io.IOException;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract tree structure printer.
 */
public abstract class IgniteTreePrinter<T> {
    /**
     * Prints root.
     *
     * @param rootNode Root tree node.
     * @param a Output.
     */
    public void print(T rootNode, Appendable a) throws IOException {
        printTree(rootNode, "", true, a);
    }

    /**
     * Returns string representation of the tree.
     *
     * @param rootNode Root tree node.
     */
    public String print(T rootNode) {
        StringBuilder b = new StringBuilder();

        try {
            print(rootNode, b);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        return b.toString();
    }

    /**
     * Prints a tree.
     *
     * @param treeNode Tree node.
     * @param prefix Prefix.
     * @param tail Tail child.
     * @param a Output.
     * @throws IOException If failed.
     */
    private void printTree(T treeNode, String prefix, boolean tail, Appendable a) throws IOException {
        List<T> children = getChildren(treeNode);

        int cnt = children == null ? 0 : children.size();

        a.append(prefix).append(tail ? "└── " : "├── ").append(formatTreeNode(treeNode)).append('\n');

        String childPrefix = prefix + (tail ? "    " : "│   ");

        if (children == null) {
            a.append(childPrefix).append("└── <list of children is not accessible>\n");
        } else {
            for (int i = 0; i < cnt; i++) {
                printTree(children.get(i), childPrefix, i == cnt - 1, a);
            }
        }
    }

    /**
     * Returns list of tree node children.
     *
     * @param treeNode The tree node.
     * @return List of children (possibly empty, if it is a leaf page) or {@code null} if the node can't be read (e.g., is locked).
     */
    protected abstract @Nullable List<T> getChildren(T treeNode);

    /**
     * Returns string representation.
     *
     * @param treeNode Tree node.
     */
    protected abstract String formatTreeNode(T treeNode);
}
