package org.apache.ignite.lang;

import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.lang.IgniteExceptionMapper.checked;
import static org.apache.ignite.lang.IgniteExceptionMapper.unchecked;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.lang.IgniteExceptionMapperUtilTest.CustomInternalCheckedException;
import org.apache.ignite.lang.IgniteExceptionMapperUtilTest.CustomInternalException;

/**
 * This class represents a test exception mapper.
 */
@AutoService(IgniteExceptionMappersProvider.class)
public class TestIgniteExceptionMappersProvider implements IgniteExceptionMappersProvider {
    @Override
    public Collection<IgniteExceptionMapper<? extends Exception, ? extends Exception>> mappers() {
        List<IgniteExceptionMapper<? extends Exception, ? extends Exception>> mappers = new ArrayList<>();

        mappers.add(
                unchecked(
                        CustomInternalException.class,
                        err -> new IgniteException(err.traceId(), NODE_STOPPING_ERR, err)));

        mappers.add(
                checked(
                        CustomInternalCheckedException.class,
                        err -> new IgniteCheckedException(err.traceId(), NODE_STOPPING_ERR, err)));

        return mappers;
    }
}
