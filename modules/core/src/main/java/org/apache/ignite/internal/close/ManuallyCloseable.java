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

package org.apache.ignite.internal.close;

/**
 * Represents something that must be eventually closed. It is different from {@link AutoCloseable} which is for being
 * used in try-with-resources; IDEs treat any usage of an AutoCloseable outside of a try-with-resources block as a suspicious
 * and issue a warning, so it becomes a drag to work with code that uses AutoCloseable for classes which instances
 * are used with patterns different from the try-with-resources pattern.
 *
 * <p>The main reason of this interface appearance was the desire to mark 'must-be-eventually-closed' types so that
 * we don't forget closing their instances.
 *
 * <p>Subinterfaces and classes implementing this interface should declare a narrower {@code throws} declaration (either
 * having concrete subclasses of {@link Exception} or declaring no exceptions at all).
 */
public interface ManuallyCloseable {
    /**
     * Closes the object.
     *
     * @throws Exception If something fails during the closure.
     */
    void close() throws Exception;
}
