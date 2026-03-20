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

package org.apache.ignite.internal.cli.core.exception.handler;

import java.io.IOError;
import java.util.function.Consumer;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;

/**
 * Exception handler for {@link IOError}.
 *
 * <p>JLine throws {@link IOError} wrapping {@link java.io.IOException} when terminal I/O fails
 * (e.g., the parent shell process is killed and the PTY becomes broken). Without this handler, the REPL loop would spin indefinitely
 * printing "Unknown error" on every iteration.
 */
class IoErrorExceptionHandler implements ExceptionHandler<IOError> {
    private final Consumer<Boolean> endAction;

    /**
     * Constructor.
     *
     * @param endAction action to stop the REPL loop.
     */
    IoErrorExceptionHandler(Consumer<Boolean> endAction) {
        this.endAction = endAction;
    }

    @Override
    public int handle(ExceptionWriter err, IOError e) {
        endAction.accept(true);
        return 1;
    }

    @Override
    public Class<IOError> applicableException() {
        return IOError.class;
    }
}
