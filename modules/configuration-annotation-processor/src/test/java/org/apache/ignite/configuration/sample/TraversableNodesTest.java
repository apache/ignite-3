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

package org.apache.ignite.configuration.sample;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.sample.impl.ChildNode;
import org.apache.ignite.configuration.sample.impl.NamedElementNode;
import org.apache.ignite.configuration.sample.impl.ParentNode;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.NamedListNode;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptySet;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** */
public class TraversableNodesTest {
    /** */
    @Config
    public static class ParentConfigurationSchema {
        /** */
        @ConfigValue
        private ChildConfigurationSchema child;

        /** */
        @NamedConfigValue
        private NamedElementConfigurationSchema elements;
    }

    /** */
    @Config
    public static class ChildConfigurationSchema {
        /** */
        @Value(immutable = true)
        private int intCfg;

        /** */
        @Value
        private String strCfg;
    }

    /** */
    @Config
    public static class NamedElementConfigurationSchema {
        /** */
        @Value
        private String strCfg;
    }

    /** */
    private static class VisitException extends RuntimeException {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;
    }

    /** */
    @Test
    public void nodeClassesImplementRequiredInterfaces() {
        var parentNode = new ParentNode();

        assertThat(parentNode, instanceOf(ParentView.class));
        assertThat(parentNode, instanceOf(ParentChange.class));
        assertThat(parentNode, instanceOf(ParentInit.class));

        var namedElementNode = new NamedElementNode();

        assertThat(namedElementNode, instanceOf(NamedElementView.class));
        assertThat(namedElementNode, instanceOf(NamedElementChange.class));
        assertThat(namedElementNode, instanceOf(NamedElementInit.class));

        var childNode = new ChildNode();

        assertThat(childNode, instanceOf(ChildView.class));
        assertThat(childNode, instanceOf(ChildChange.class));
        assertThat(childNode, instanceOf(ChildInit.class));
    }

    /** */
    @Test
    public void changeLeaf() {
        var childNode = new ChildNode();

        assertNull(childNode.strCfg());

        childNode.changeStrCfg("value");

        assertEquals("value", childNode.strCfg());
    }

    /** */
    @Test
    public void changeInnerChild() {
        var parentNode = new ParentNode();

        assertNull(parentNode.child());

        parentNode.changeChild(child -> {});

        ChildNode childNode = parentNode.child();

        assertNotNull(childNode);

        parentNode.changeChild(child -> child.changeStrCfg("value"));

        assertSame(childNode, parentNode.child());
    }

    /** */
    @Test
    public void changeNamedChild() {
        var parentNode = new ParentNode();

        NamedListNode<NamedElementNode> elementsNode = parentNode.elements();

        assertNotNull(elementsNode);

        parentNode.changeElements(elements -> elements.put("key", element -> {}));

        assertSame(elementsNode, parentNode.elements());
    }

    /** */
    @Test
    public void initLeaf() {
        var childNode = new ChildNode();

        childNode.initStrCfg("value");

        assertEquals("value", childNode.strCfg());
    }

    /** */
    @Test
    public void initInnerChild() {
        var parentNode = new ParentNode();

        parentNode.initChild(child -> {});

        ChildNode childNode = parentNode.child();

        parentNode.initChild(child -> child.initStrCfg("value"));

        assertSame(childNode, parentNode.child());
    }

    /** */
    @Test
    public void initNamedChild() {
        var parentNode = new ParentNode();

        NamedListNode<NamedElementNode> elementsNode = parentNode.elements();

        parentNode.initElements(elements -> elements.put("key", element -> {}));

        assertSame(elementsNode, parentNode.elements());
    }

    /** */
    @Test
    public void changeOrInitNamedConfiguration() {
        var elementsNode = new NamedListNode<>(NamedElementNode::new);

        assertEquals(emptySet(), elementsNode.namedListKeys());

        elementsNode.put("keyPut", element -> {});

        assertThat(elementsNode.namedListKeys(), hasItem("keyPut"));

        NamedElementNode elementNode = elementsNode.get("keyPut");

        assertNotNull(elementNode);

        assertNull(elementNode.strCfg());

        elementsNode.put("keyPut", element -> element.changeStrCfg("val"));

        assertSame(elementNode, elementsNode.get("keyPut"));

        assertEquals("val", elementNode.strCfg());

        assertThrows(IllegalStateException.class, () -> elementsNode.remove("keyPut"));

        elementsNode.remove("keyRemove");

        assertThat(elementsNode.namedListKeys(), hasItem("keyRemove"));

        assertNull(elementsNode.get("keyRemove"));

        assertThrows(IllegalStateException.class, () -> elementsNode.put("keyRemove", element -> {}));
    }

    /** */
    @Test
    public void innerNodeAcceptVisitor() {
        var parentNode = new ParentNode();

        assertThrows(VisitException.class, () ->
            parentNode.accept("root", new ConfigurationVisitor() {
                @Override public void visitInnerNode(String key, InnerNode node) {
                    throw new VisitException();
                }
            })
        );
    }

    /** */
    @Test
    public void namedListNodeAcceptVisitor() {
        var elementsNode = new NamedListNode<>(NamedElementNode::new);

        assertThrows(VisitException.class, () ->
            elementsNode.accept("root", new ConfigurationVisitor() {
                @Override public <N extends InnerNode> void visitNamedListNode(String key, NamedListNode<N> node) {
                    throw new VisitException();
                }
            })
        );
    }

    /** */
    @Test
    public void traverseChildren() {
        var parentNode = new ParentNode();

        List<String> keys = new ArrayList<>(2);

        parentNode.traverseChildren(new ConfigurationVisitor() {
            @Override public void visitInnerNode(String key, InnerNode node) {
                assertNull(node);

                assertEquals("child", key);

                keys.add(key);
            }

            @Override public <N extends InnerNode> void visitNamedListNode(String key, NamedListNode<N> node) {
                assertEquals("elements", key);

                keys.add(key);
            }
        });

        assertEquals(List.of("child", "elements"), keys);

        keys.clear();

        ChildNode childNode = new ChildNode();

        childNode.traverseChildren(new ConfigurationVisitor() {
            @Override public void visitLeafNode(String key, Serializable val) {
                keys.add(key);
            }
        });

        assertEquals(List.of("intCfg", "strCfg"), keys);
    }

    /** */
    @Test
    public void traverseSingleChild() {
        var parentNode = new ParentNode();

        assertThrows(VisitException.class, () ->
            parentNode.traverseChild("child", new ConfigurationVisitor() {
                @Override public void visitInnerNode(String key, InnerNode node) {
                    assertEquals("child", key);

                    throw new VisitException();
                }
            })
        );

        assertThrows(VisitException.class, () ->
            parentNode.traverseChild("elements", new ConfigurationVisitor() {
                @Override
                public <N extends InnerNode> void visitNamedListNode(String key, NamedListNode<N> node) {
                    assertEquals("elements", key);

                    throw new VisitException();
                }
            })
        );

        var childNode = new ChildNode();

        assertThrows(VisitException.class, () ->
            childNode.traverseChild("intCfg", new ConfigurationVisitor() {
                @Override public void visitLeafNode(String key, Serializable val) {
                    assertEquals("intCfg", key);

                    throw new VisitException();
                }
            })
        );

        assertThrows(NoSuchElementException.class, () ->
            childNode.traverseChild("foo", new ConfigurationVisitor() {})
        );
    }
}
