package org.apache.ignite.internal.sql.engine.exec;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.externalize.RelJson;
import org.junit.jupiter.api.Test;

public class RelJsonTest {
    @Test
    public void sargSerialization() {
        RelJson relJson = new RelJson();

        RelDataType binaryType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARBINARY);

        byte[] bytes1 = {1};
        byte[] bytes2 = {2};

        Range<ByteString> closedBinaryRange = Range.closed(new ByteString(bytes1), new ByteString(bytes2));
        RangeSet<ByteString> closedRangeSetBinary = ImmutableRangeSet.of(closedBinaryRange);

        assertThat(RelJson.rangeSetFromJson(relJson.toJson(closedRangeSetBinary)),
                is(closedRangeSetBinary));
    }
}
