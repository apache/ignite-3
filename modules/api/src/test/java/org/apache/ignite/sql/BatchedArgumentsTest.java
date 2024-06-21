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

package org.apache.ignite.sql;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Tests for {@link BatchedArguments}.
 */
public class BatchedArgumentsTest {
    @Test
    public void nullAndEmptyArgumentsAreForbidden() {
        assertThrows(NullPointerException.class, "args", () -> BatchedArguments.of((Object[]) null));
        assertThrows(NullPointerException.class, "batchedArgs", () -> BatchedArguments.of((List<List<Object>>) null));
        assertThrows(NullPointerException.class, "Arguments list cannot be null.", () -> BatchedArguments.of(singletonList(null)));
        assertThrows(IllegalArgumentException.class, "Non empty arguments required.", () -> BatchedArguments.of(new Object[0]));

        BatchedArguments batch = BatchedArguments.create();
        assertThrows(NullPointerException.class, "args", () -> batch.add((Object[]) null));
        assertThrows(NullPointerException.class, "argsList", () -> batch.add((List<Object>) null));
        assertThrows(IllegalArgumentException.class, "Non empty arguments required.", () -> batch.add(new Object[0]));
        assertThrows(IllegalArgumentException.class, "Non empty arguments required.", () -> batch.add(List.of()));
    }

    @Test
    public void iteratorIsImmutable() {
        List<List<Object>> argLists = new ArrayList<>(2);
        argLists.add(List.of(1, 2));
        argLists.add(List.of(3, 4));

        BatchedArguments batch = BatchedArguments.of(argLists);
        assertThat(batch.size(), is(argLists.size()));

        Iterator<List<Object>> itr = batch.iterator();
        assertThat(itr.next(), equalTo(argLists.get(0)));
        assertThat(itr.hasNext(), is(true));
        assertThat(itr.next(), equalTo(argLists.get(1)));
        assertThat(itr.hasNext(), is(false));

        Assertions.assertThrows(UnsupportedOperationException.class, itr::remove);

        assertThat(batch.size(), is(argLists.size()));
    }

    @Test
    public void argumentsListsMustBeSameSize() {
        List<List<Object>> argLists = new ArrayList<>(2);
        argLists.add(List.of(1));
        argLists.add(List.of(2, 3));

        assertThrows(IllegalArgumentException.class, "Argument lists must be the same size.", () -> BatchedArguments.of(argLists));

        {
            BatchedArguments batch = BatchedArguments.of(1);
            assertThrows(IllegalArgumentException.class, "Argument lists must be the same size.", () -> batch.add(1, 2));
        }

        {
            BatchedArguments batch = BatchedArguments.of(1);
            assertThrows(IllegalArgumentException.class, "Argument lists must be the same size.", () -> batch.add(List.of(1, 2)));
        }
    }

    @Test
    public void argumentsListsAreImmutable() {
        List<List<Object>> argLists = new ArrayList<>(2);
        argLists.add(List.of(1));
        argLists.add(List.of(2));

        BatchedArguments batch = BatchedArguments.of(argLists);
        assertThat(batch.size(), is(argLists.size()));

        argLists.add(List.of(3));
        assertThat(batch.size(), is(argLists.size() - 1));

        Assertions.assertThrows(UnsupportedOperationException.class, () -> batch.get(0).add("2"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> batch.get(0).remove(0));

        List<Object> modifiableList = new ArrayList<>(List.of("John"));
        batch.add(modifiableList);

        assertThat(batch.get(2), equalTo(modifiableList));
        modifiableList.add("Mary");

        assertThat(batch.get(2), not(equalTo(modifiableList)));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> batch.get(2).add("Mary"));
    }

    private static <T extends Throwable> void assertThrows(Class<T> expectedType, String expMsg, Executable executable) {
        T ex = Assertions.assertThrows(expectedType, executable);

        assertThat(ex.getMessage(), containsString(expMsg));
    }
}
