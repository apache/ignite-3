/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.cli.exceptions;

import org.apache.ignite3.client.IgniteClientConnectionException;
import org.apache.ignite3.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite3.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite3.internal.cli.core.style.component.ErrorUiComponent;

/** Simple exception handler for {@link IgniteClientConnectionException}. */
public class IgniteClientConnectionExceptionHandler implements ExceptionHandler<IgniteClientConnectionException> {
    @Override
    public int handle(ExceptionWriter writer, IgniteClientConnectionException e) {
        writer.write(ErrorUiComponent.fromHeader(e.getMessage()).render());
        return e.code();
    }

    @Override
    public Class<IgniteClientConnectionException> applicableException() {
        return IgniteClientConnectionException.class;
    }
}
