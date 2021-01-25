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

package org.apache.ignite.configuration.poc;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/** */
public interface Visitor {
    /** */
    void visitSimpleNode(String key, SimpleNode node);

    /** */
    <T> void visitNamedListNode(String key, NamedListNode<T> node);

    /** */
    void visitLeafNode(String key, Serializable val);
}

/** */
interface Traversable {
    /** */
    void accept(String key, Visitor v);

    /** */
    void traverseChildren(Visitor v);
}

/** */
abstract class SimpleNode implements Traversable {
    /** {@inheritDoc} */
    @Override public final void accept(String key, Visitor v) {
        v.visitSimpleNode(key, this);
    }
}

/** */
abstract class NamedListNode<T> implements Traversable, NamedListChange<T> {
    /** */
    protected final Map<String, T> map = new HashMap<>();

    /** {@inheritDoc} */
    @Override public final void accept(String key, Visitor v) {
        v.visitNamedListNode(key, this);
    }

    /** {@inheritDoc} */
    @Override public final void traverseChildren(Visitor v) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Set<String> namedListKeys() {
        return Collections.unmodifiableSet(map.keySet());
    }

    /** */
    protected abstract Traversable getTraversable(String key);

    /** {@inheritDoc} */
    @Override public T get(String key) {
        if (!map.containsKey(key))
            throw new NoSuchElementException(key);

        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<T> put(String key, T val) {
        Objects.requireNonNull(val, "val");

        if (map.containsKey(key) && map.get(key) == null)
            throw new IllegalStateException("You can't add entity that has just been deleted.");

        map.put(key, val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<T> remove(String key) {
        if (map.containsKey(key) && map.get(key) != null)
            throw new IllegalStateException("You can't add entity that has just been modified.");

        map.put(key, null);

        return this;
    }
}

/** */
interface NamedListView<T> {
    /** */
    Set<String> namedListKeys();

    /** */
    T get(String key);
}

/** */
interface NamedListChange<T> extends NamedListView<T> {
    /** */
    NamedListChange<T> put(String key, T val);

    /** */
    NamedListChange<T> remove(String key);
}

/** */
class NamedListNodeTraversable<T extends Traversable> extends NamedListNode<T> {
    /** */
    @Override protected Traversable getTraversable(String key) {
        return get(key);
    }
}

/** */
class NamedListNodeSerializable<T extends Serializable> extends NamedListNode<T> {
    /** */
    @Override protected Traversable getTraversable(String key) {
        return new LeafNode(get(key));
    }
}

/** Virtual node to unify traversing. */
class LeafNode implements Traversable {
    /** */
    private final Serializable val;

    /** */
    public LeafNode(Serializable val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public final void accept(String key, Visitor v) {
        v.visitLeafNode(key, val);
    }

    /** {@inheritDoc} */
    @Override public final void traverseChildren(Visitor v) {
        // No-op.
    }
}

/** */
interface Constructable {
    /** */
    void initChild(String key, Source src);

    /*
     * Simple:
     *      switch (key) {
     *          case "a":
     *              src.descend(a = (a == null ? new AImpl() : a.clone()));
     *              break;
     *
     *          case "b":
     *              b = src.unwrap(Integet.class);
     *              break;
     *
     *          default:
     *              src.notFound(key);
     *      }
     *
     * NamedList:
     *      ValImpl val = map.get(key);
     *      val = (val == null) ? new ValImpl() : val.clone();
     *      map.put(key);
     *      src.descend(val);
     * or
     *      map.put(key, src.unwrap(valueClass());
     */
}

/** */
interface Source {
    /** */
    <T extends Serializable> T unwrap(Class<T> clazz);

    /** */
    void descend(Constructable c);
//        for (String key : this.subkeys())
//            c.initChild(key, this.get(key));

    /** */
    default void keyNotFound(String key) {}
}

/** */
interface X {
    /** */
    static <C extends Constructable> C toTree(Map<String, Serializable> map, Class<C> clazz) {
        C root = instantiate(clazz);

        for (Map.Entry<String, Serializable> entry : map.entrySet()) {
            List<String> keys = new LinkedList<>(Arrays.asList(entry.getKey().split("(?<!\\\\)[.]")));
//            C root = getRoot(keys.remove(0));

            Source src = new Source() {
                /** {@inheritDoc} */
                @Override public <T extends Serializable> T unwrap(Class<T> clazz) {
                    assert keys.isEmpty();

                    return clazz.cast(entry.getValue());
                }

                /** {@inheritDoc} */
                @Override public void descend(Constructable c) {
                    assert !keys.isEmpty();

                    String key = keys.remove(0);

                    c.initChild(unescape(key), this);
                }
            };

            src.descend(root);
        }

        return root;
    }

    /** */
    static String unescape(String key) {
        return key; //TODO Implement.
    }

    /** */
    private static <T> T instantiate(Class<T> clazz) {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e); //TODO Change exception type.
        }
    }

    static <N extends Traversable & Constructable> void merge(N to, N from) {
        Source src = new SimpleSource((SimpleNode)from);

        src.descend(to);
    }

    static class LeafSource implements Source {
        final Serializable val;

        public LeafSource(Serializable val) {
            this.val = val;
        }

        @Override public <T extends Serializable> T unwrap(Class<T> clazz) {
            return clazz.cast(val);
        }

        @Override public void descend(Constructable c) {
            throw new UnsupportedOperationException("descend");
        }
    }

    static class SimpleSource implements Source {
        final SimpleNode node;

        public SimpleSource(SimpleNode node) {
            this.node = node;
        }

        @Override public <T extends Serializable> T unwrap(Class<T> clazz) {
            throw new UnsupportedOperationException("unwrap");
        }

        @Override public void descend(Constructable c) {
            node.traverseChildren(new Visitor() {
                @Override public void visitSimpleNode(String key, SimpleNode node) {
                    c.initChild(key, new SimpleSource(node));
                }

                @Override public <T> void visitNamedListNode(String key, NamedListNode<T> node) {
                    if (node instanceof NamedListNodeSerializable)
                        c.initChild(key, new NamedListSourceSerializable<>((NamedListNodeSerializable)node)); // Bullshit
                    else
                        c.initChild(key, new NamedListSourceTraversable<>((NamedListNodeTraversable)node));
                }

                @Override public void visitLeafNode(String key, Serializable val) {
                    c.initChild(key, new LeafSource(val));
                }
            });
        }
    }

    static class NamedListSourceSerializable<T extends Serializable> implements Source {
        final NamedListNodeSerializable<T> node;

        public NamedListSourceSerializable(NamedListNodeSerializable<T> node) {
            this.node = node;
        }

        @Override public <T extends Serializable> T unwrap(Class<T> clazz) {
            throw new UnsupportedOperationException("unwrap");
        }

        @Override public void descend(Constructable c) {
            for (String key : node.namedListKeys()) {
                T val = node.get(key);

                c.initChild(key, new LeafSource(val));
            }
        }
    }

    static class NamedListSourceTraversable<T extends Traversable> implements Source {
        final NamedListNodeTraversable<T> node;

        public NamedListSourceTraversable(NamedListNodeTraversable<T> node) {
            this.node = node;
        }

        @Override public <T extends Serializable> T unwrap(Class<T> clazz) {
            throw new UnsupportedOperationException("unwrap");
        }

        @Override public void descend(Constructable c) {
            for (String key : node.namedListKeys()) {
                T val = node.get(key);

                c.initChild(key, new SimpleSource((SimpleNode)val));
            }
        }
    }
}