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

import java.io.FileNotFoundException;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.style.component.ErrorUiComponent;

/** Exception handler for {@link FileNotFoundException}. */
public class FileNotFoundExceptionHandler implements ExceptionHandler<FileNotFoundException> {
    @Override
    public int handle(ExceptionWriter err, FileNotFoundException e) {
        err.write(
                ErrorUiComponent.builder()
                        .header("No such file or directory")
                        .details(e.getMessage())
                        .build().render()
        );

        return 1;
    }

    /** {@inheritDoc} */
    @Override
    public Class<FileNotFoundException> applicableException() {
        return FileNotFoundException.class;
    }
}
