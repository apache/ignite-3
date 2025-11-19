<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
|
    { return false; }
}

SqlDataTypeSpec DataTypeEx(Span s, boolean allowCharType, boolean allowBinaryType) :
{
    final SqlDataTypeSpec dt;
}
{
    (
        dt = DataType()
    |
        dt = IntervalType()
    )
    {
        if(dt.getTypeName().isSimple()) {
            String typeName = dt.getTypeName().getSimple();
            if (!allowCharType && ("CHAR".equals(typeName) || "CHARACTER".equals(typeName))) {
               throw SqlUtil.newContextException(s.pos(), IgniteResource.INSTANCE.charDataTypeIsNotSupportedInTable());
            }
            if (!allowBinaryType && "BINARY".equals(typeName)) {
                throw SqlUtil.newContextException(s.pos(), IgniteResource.INSTANCE.binaryDataTypeIsNotSupportedInTable());
            }
        }
        return dt;
    }
}

SqlDataTypeSpec IntervalType() :
{
    final Span s;
    final SqlIntervalQualifier intervalQualifier;
}
{
    <INTERVAL> { s = span(); } intervalQualifier = IntervalQualifier() {
        return new SqlDataTypeSpec(new IgniteSqlIntervalTypeNameSpec(intervalQualifier, s.end(this)), s.pos());
    }
}

void TableElement(List<SqlNode> list) :
{
    final SqlDataTypeSpec type;
    final Boolean nullable;
    final Span s = Span.of();
    final ColumnStrategy strategy;
    final SqlNode dflt;
    SqlIdentifier id = null;
    SqlNodeList columnList = new SqlNodeList(s.end(this));
    IgniteSqlPrimaryKeyIndexType primaryIndexType = IgniteSqlPrimaryKeyIndexType.IMPLICIT_HASH;
}
{
    id = SimpleIdentifier() type = DataTypeEx(s, false, false) nullable = NullableOptDefaultNull()
    (
        <DEFAULT_> { s.add(this); }
        (
            dflt = DefaultExpression()
        )  {
            strategy = ColumnStrategy.DEFAULT;
        }
    |
        {
            dflt = null;
            strategy = nullable == null
                ? null
                : nullable
                    ? ColumnStrategy.NULLABLE
                    : ColumnStrategy.NOT_NULLABLE;
        }
    )
    [
        <PRIMARY> { s.add(this); } <KEY> {
            columnList = SqlNodeList.of(id);
            list.add(new IgniteSqlPrimaryKeyConstraint(s.end(columnList), null, columnList, primaryIndexType));
        }
    ]
    {
        list.add(
            SqlDdlNodes.column(s.add(id).end(this), id,
                type.withNullable(nullable), dflt, strategy));
    }
|
    [ <CONSTRAINT> { s.add(this); } id = SimpleIdentifier() ]
    <PRIMARY> { s.add(this); } <KEY>
    (
        LOOKAHEAD(2)
        <USING> <SORTED> {
            s.add(this);
            primaryIndexType = IgniteSqlPrimaryKeyIndexType.SORTED;
        }
        columnList = ColumnNameWithSortDirectionList()
     |
        LOOKAHEAD(2)
        <USING> <HASH> {
            s.add(this);
            primaryIndexType = IgniteSqlPrimaryKeyIndexType.HASH;
        }
        columnList = ColumnNameList()
    |
       columnList = ParenthesizedSimpleIdentifierList()
    ) {
       list.add(new IgniteSqlPrimaryKeyConstraint(s.end(columnList), id, columnList, primaryIndexType));
    }
}

/**
* Parse a nullable option, default is null.
*/
Boolean NullableOptDefaultNull() :
{
}
{
    <NULL> { return true; }
    |
    <NOT> <NULL> { return false; }
    |
    { return null; }
}


SqlNodeList TableElementList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    TableElement(list)
    (
        <COMMA> TableElement(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNodeList columnList;
    SqlIdentifier zoneName = null;
    SqlNode storageProfile = null;
    SqlNodeList colocationColumns = null;
    SqlNodeList tableProperties = null;
}
{
    <TABLE>
    ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    columnList = TableElementList()
    [
        <COLOCATE> [<BY>] {s.add(this);}
            colocationColumns = ParenthesizedSimpleIdentifierList()
    ]
    [
        <ZONE> {s.add(this);} zoneName = SimpleIdentifier()
    ]
    [
        <STORAGE> <PROFILE> {s.add(this);} storageProfile = StringLiteral()
    ]
    [
        <WITH> {s.add(this);} tableProperties = TablePropertyList(s)
    ]
    {
        return new IgniteSqlCreateTable(
                s.end(this), ifNotExists, id, columnList, colocationColumns, zoneName, storageProfile, tableProperties
        );
    }
}

SqlNodeList TablePropertyList(Span s) :
{
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode property;
}
{
    <LPAREN> { s.add(this); }
    property = TableProperty(s) {
        list.add(property);
    }
    (
        <COMMA> property = TableProperty(s) {
           list.add(property);
       }
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlNode TableProperty(Span s) :
{
    IgniteSqlTablePropertyKey key;
    SqlNode value;
    SqlParserPos pos;
}
{
    (
        <MIN> { pos = getPos(); } <STALE> <ROWS>
        value = UnsignedIntegerLiteral() {
            key = IgniteSqlTablePropertyKey.MIN_STALE_ROWS_COUNT;
            return new IgniteSqlTableProperty(key.symbol(getPos()), value, s.end(this));
        }
    |
        <STALE> { pos = getPos(); } <ROWS> <FRACTION> value = UnsignedNumericLiteral()
        {
            key = IgniteSqlTablePropertyKey.STALE_ROWS_FRACTION;
            return new IgniteSqlTableProperty(key.symbol(getPos()), value, s.end(this));
        }
    )
}

SqlNode ColumnNameWithSortDirection() :
{
    final Span s;
    SqlNode col;
}
{
    col = SimpleIdentifier()
    (
        <ASC>
    |   <DESC> {
            col = SqlStdOperatorTable.DESC.createCall(getPos(), col);
        }
    )?

    (
        LOOKAHEAD(2)
        <NULLS> <FIRST> {
            col = SqlStdOperatorTable.NULLS_FIRST.createCall(getPos(), col);
        }
    |
        <NULLS> <LAST> {
            col = SqlStdOperatorTable.NULLS_LAST.createCall(getPos(), col);
        }
    )?

    {
        return col;
    }
}

SqlNodeList ColumnNameWithSortDirectionList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode col = null;
}
{
    <LPAREN> { s = span(); }
    col = ColumnNameWithSortDirection() { list.add(col); }
    (
        <COMMA> col = ColumnNameWithSortDirection() { list.add(col); }
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlNodeList ColumnNameList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode col = null;
}
{
    <LPAREN> { s = span(); }
    col = SimpleIdentifier() { list.add(col); }
    (
        <COMMA> col = SimpleIdentifier() { list.add(col); }
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlCreate SqlCreateIndex(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier idxId;
    final SqlIdentifier tblId;
    final SqlNodeList columnList;
    IgniteSqlIndexType type = IgniteSqlIndexType.IMPLICIT_SORTED;
}
{
    <INDEX>
    ifNotExists = IfNotExistsOpt()
    idxId = SimpleIdentifier()
    <ON>
    tblId = CompoundIdentifier()
    (
        columnList = ColumnNameWithSortDirectionList()
    |
        LOOKAHEAD(2)
        <USING> <SORTED> {
            s.add(this);

            type = IgniteSqlIndexType.SORTED;
        }

        columnList = ColumnNameWithSortDirectionList()
    |
        <USING> <HASH> {
            s.add(this);

            type = IgniteSqlIndexType.HASH;
        }

        columnList = ColumnNameList()
    ) {
        return new IgniteSqlCreateIndex(s.end(this), ifNotExists, idxId, tblId, type, columnList);
    }
}

SqlCreate SqlCreateSchema(Span s, boolean replace) :
{
        final boolean ifNotExists;
        final SqlIdentifier id;
}
{
    <SCHEMA> { s.add(this); }
        ifNotExists = IfNotExistsOpt()
        id = CompoundIdentifier()
    {
        return new IgniteSqlCreateSchema(s.end(this), ifNotExists, id);
    }
}

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
}

SqlDrop SqlDropTable(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return new IgniteSqlDropTable(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropIndex(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier idxId;
}
{
    <INDEX> ifExists = IfExistsOpt() idxId = CompoundIdentifier() {
        return new IgniteSqlDropIndex(s.end(this), ifExists, idxId);
    }
}

SqlDrop SqlDropSchema(Span s, boolean replace) :
{
    final SqlIdentifier schemaName;
    final boolean ifExists;
    IgniteSqlDropSchemaBehavior dropBehavior = IgniteSqlDropSchemaBehavior.IMPLICIT_RESTRICT;
}
{
    <SCHEMA> { s.add(this); }
        ifExists = IfExistsOpt()
        schemaName = CompoundIdentifier()
    [
        (
          <CASCADE> {
            dropBehavior = IgniteSqlDropSchemaBehavior.CASCADE;
          }
          |
          <RESTRICT> {
            dropBehavior = IgniteSqlDropSchemaBehavior.RESTRICT;
          }
        )
    ]
    {
        return new IgniteSqlDropSchema(s.end(this), ifExists, schemaName, dropBehavior);
    }
}

void InfixCast(List<Object> list, ExprContext exprContext, Span s) :
{
    final SqlDataTypeSpec dt;
}
{
    <INFIX_CAST> {
        checkNonQueryExpression(exprContext);
    }
    dt = DataTypeEx(s, true, true) {
        list.add(
            new SqlParserUtil.ToTreeListItem(SqlLibraryOperators.INFIX_CAST,
                s.pos()));
        list.add(dt);
    }
}

SqlNodeList ColumnWithTypeList() :
{
    final Span s;
    List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode col;
}
{
    <LPAREN> { s = span(); }
    col = ColumnWithType() { list.add(col); }
    (
        <COMMA> col = ColumnWithType() { list.add(col); }
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlNode ColumnWithType() :
{
    SqlIdentifier id;
    SqlDataTypeSpec type;
    boolean nullable = true;
    final ColumnStrategy strategy;
    final SqlNode dflt;
    final Span s = Span.of();
}
{
    id = SimpleIdentifier() { s.add(this); }
    type = DataTypeEx(s, false, false)
    [
        <NOT> <NULL> {
            nullable = false;
        }
        |
        <NULL> {
            nullable = true;
        }
    ]
    (
        <DEFAULT_> { s.add(this); } dflt = DefaultExpression() {
            strategy = ColumnStrategy.DEFAULT;
        }
    |
    {
            dflt = null;
            strategy = nullable ? ColumnStrategy.NULLABLE
                : ColumnStrategy.NOT_NULLABLE;
        }
    )
    {
        return SqlDdlNodes.column(s.add(id).end(this), id, type.withNullable(nullable), dflt, strategy);
    }
}

SqlNode DefaultExpression():
{
   SqlNode node;
} {
   node = Expression(ExprContext.ACCEPT_NON_QUERY) { return node; }
}

SqlNodeList ColumnWithTypeOrList() :
{
    SqlNode col;
    SqlNodeList list;
}
{
    col = ColumnWithType() { return new SqlNodeList(Collections.singletonList(col), col.getParserPosition()); }
|
    list = ColumnWithTypeList() { return list; }
}

SqlNodeList SingleTablePropertyOrList(Span s) :
{ }
{
    (
        LOOKAHEAD(<LPAREN>)
        { return TablePropertyList(s); }
    |
        { return new SqlNodeList(List.of(TableProperty(s)), s.end(this)); }
    )
}

SqlNode SqlAlterTable() :
{
    final Span s;
    final boolean ifExists;
    final SqlIdentifier id;
    boolean colIgnoreErr;
    SqlNode col;
    SqlNodeList cols;
    SqlNodeList propertyList;
}
{
    <ALTER> { s = span(); }
    <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier()
    (
        <ADD> [<COLUMN>] cols = ColumnWithTypeOrList() {
            return new IgniteSqlAlterTableAddColumn(s.end(this), ifExists, id, cols);
        }
    |
        <DROP> [<COLUMN>] cols = SimpleIdentifierOrList() {
            return new IgniteSqlAlterTableDropColumn(s.end(this), ifExists, id, cols);
        }
    |
        <ALTER> [<COLUMN>] {
            return SqlAlterColumn(s, id, ifExists);
        }
    |
        <SET> propertyList = SingleTablePropertyOrList(s) {
            return new IgniteSqlAlterTableSetProperties(s.end(this), ifExists, id, propertyList);
        }
    )
}

SqlNode SqlAlterColumn(Span s, SqlIdentifier tableId, boolean ifExists) :
{
    SqlIdentifier id;
    SqlDataTypeSpec type;
    Boolean nullable;
    SqlNode dflt;
}
{
    id = SimpleIdentifier()
    (
        LOOKAHEAD(2)
        <SET> <DATA> <TYPE> { s.add(this); } type = DataTypeEx(s, false, false) nullable = NullableOptDefaultNull() dflt = DefaultLiteralOrNull() {
            return new IgniteSqlAlterColumn(s.end(this), ifExists, tableId, id, type, false, dflt, nullable == null ? null : !nullable);
        }
    |
        LOOKAHEAD(2)
        <SET> <NOT> <NULL> {
            return new IgniteSqlAlterColumn(s.end(this), ifExists, tableId, id, null, false, null, true);
        }
    |
        LOOKAHEAD(2)
        <DROP> <NOT> <NULL> {
            return new IgniteSqlAlterColumn(s.end(this), ifExists, tableId, id, null, false, null, false);
        }
    |
        <SET> <DEFAULT_> { s.add(this); } dflt = DefaultExpression()
        {
            return new IgniteSqlAlterColumn(s.end(this), ifExists, tableId, id, null, false, dflt, null);
        }
    |
        <DROP> <DEFAULT_> {
            return new IgniteSqlAlterColumn(s.end(this), ifExists, tableId, id, null, true, SqlLiteral.createNull(s.end(this)), null);
        }
    )
}

SqlNode DefaultLiteralOrNull() :
{
    SqlNode dflt;
}
{
    <DEFAULT_> dflt = DefaultExpression()
    {
        return dflt;
    }
    |
    {
        return null;
    }
}

<DEFAULT, DQID, BTID> TOKEN :
{
< NEGATE: "!" >
|   < TILDE: "~" >
}

SqlCreate SqlCreateZone(Span s, boolean replace) :
{
        final boolean ifNotExists;
        final SqlIdentifier id;
        SqlNodeList optionList = null;
}
{
    <ZONE> { s.add(this); }
        ifNotExists = IfNotExistsOpt()
        id = CompoundIdentifier()
        (
            <WITH> { s.add(this); } optionList = CreateZoneOptionList()
            {
                return IgniteSqlCreateZone.create(s.end(this), ifNotExists, id, optionList);
            }
            |
            (
                [ optionList = ZoneOptionsList() ]

                <STORAGE> <PROFILES> {
                    SqlNodeList storageProfiles = StorageProfiles();
                }
            )
            {
                return new IgniteSqlCreateZone(s.end(this), ifNotExists, id, optionList, storageProfiles);
            }
        )
}

SqlNodeList StorageProfiles() :
{
    final Span s = Span.of();
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LBRACKET>
    StorageProfileOption(list)
    (
        <COMMA> { s.add(this); } StorageProfileOption(list)
    )*
    <RBRACKET> {
        return new SqlNodeList(list, s.end(this));
    }
}

void StorageProfileOption(List<SqlNode> list) :
{
    final SqlCharStringLiteral val;
}
{
    val = NonEmptyCharacterStringLiteral() { list.add(val); }
}

SqlCharStringLiteral NonEmptyCharacterStringLiteral() :
{
}
{
    <QUOTED_STRING> {
        String val = SqlParserUtil.parseString(token.image).trim();
        if (val.isEmpty()) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.validationError("Empty character literal is not allowed in this context."));
        }
        SqlCharStringLiteral literal = SqlLiteral.createCharString(val, getPos());
        return literal;
    }
}

SqlNodeList ZoneOptionsList() :
{
    List<SqlNode> zoneOptions = new ArrayList<SqlNode>();

    final Span s;
}
{
    <LPAREN> { s = span(); s.add(this); }
    ZoneElement(zoneOptions)
    (
        <COMMA> ZoneElement(zoneOptions)
    )*
     <RPAREN>
    {
        return new SqlNodeList(zoneOptions, s.end(this));
    }
}

void ZoneElement(List<SqlNode> zoneOptions) :
{
    final Span s;
    SqlIdentifier key;
    final SqlNode option;
    final SqlParserPos pos;
}
{
  { s = span(); }
  (
      <AUTO> { pos = getPos(); }
      (
          <SCALE>
          (
              <UP>
              (
                  option = UnsignedIntegerLiteral()
                  {
                      key = new SqlIdentifier(ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_UP.name(), pos);
                      zoneOptions.add(new IgniteSqlZoneOption(key, option, s.end(this)));
                  }
                  |
                  <OFF>
                  {
                      key = new SqlIdentifier(ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_UP.name(), pos);
                      zoneOptions.add(new IgniteSqlZoneOption(key, IgniteSqlZoneOptionMode.SCALE_OFF.symbol(getPos()), s.end(this)));
                  }
              )
              |
              <DOWN>
              (
                  option = UnsignedIntegerLiteral()
                  {
                      key = new SqlIdentifier(ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_DOWN.name(), pos);
                      zoneOptions.add(new IgniteSqlZoneOption(key, option, s.end(this)));
                  }
                  |
                  <OFF>
                  {
                      key = new SqlIdentifier(ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_DOWN.name(), pos);
                      zoneOptions.add(new IgniteSqlZoneOption(key, IgniteSqlZoneOptionMode.SCALE_OFF.symbol(getPos()), s.end(this)));
                  }
              )
              |
              <OFF>
              {
                  key = new SqlIdentifier(ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_UP.name(), pos);
                  zoneOptions.add(new IgniteSqlZoneOption(key, IgniteSqlZoneOptionMode.SCALE_OFF.symbol(getPos()), s.end(this)));

                  key = new SqlIdentifier(ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_DOWN.name(), pos);
                  zoneOptions.add(new IgniteSqlZoneOption(key, IgniteSqlZoneOptionMode.SCALE_OFF.symbol(getPos()), s.end(this)));
              }
          )
      )
      |
      <PARTITIONS> { pos = getPos(); } option = UnsignedIntegerLiteral()
      {
          key = new SqlIdentifier(ZoneOptionEnum.PARTITIONS.name(), pos);
          zoneOptions.add(new IgniteSqlZoneOption(key, option, s.end(this)));
      }
      |
      <REPLICAS> { pos = getPos(); }
      (
          option = UnsignedIntegerLiteral()
          {
              key = new SqlIdentifier(ZoneOptionEnum.REPLICAS.name(), pos);
              zoneOptions.add(new IgniteSqlZoneOption(key, option, s.end(this)));
          }
          |
          <ALL>
          {
              key = new SqlIdentifier(ZoneOptionEnum.REPLICAS.name(), pos);
              zoneOptions.add(new IgniteSqlZoneOption(key, IgniteSqlZoneOptionMode.ALL.symbol(getPos()), s.end(this)));
          }
      )
      |
      <QUORUM> { pos = getPos(); } <SIZE> option = UnsignedIntegerLiteral()
      {
          key = new SqlIdentifier(ZoneOptionEnum.QUORUM_SIZE.name(), pos);
          zoneOptions.add(new IgniteSqlZoneOption(key, option, s.end(this)));
      }
      |
      <DISTRIBUTION> { pos = getPos(); } <ALGORITHM> option = NonEmptyCharacterStringLiteral()
      {
          key = new SqlIdentifier(ZoneOptionEnum.DISTRIBUTION_ALGORITHM.name(), pos);
          zoneOptions.add(new IgniteSqlZoneOption(key, option, s.end(this)));
      }
      |
      <NODES> { pos = getPos(); } <FILTER> option = NonEmptyCharacterStringLiteral()
      {
          key = new SqlIdentifier(ZoneOptionEnum.DATA_NODES_FILTER.name(), pos);
          zoneOptions.add(new IgniteSqlZoneOption(key, option, s.end(this)));
      }
      |
      <CONSISTENCY> { pos = getPos(); } <MODE> option = NonEmptyCharacterStringLiteral()
      {
          key = new SqlIdentifier(ZoneOptionEnum.CONSISTENCY_MODE.name(), pos);
          zoneOptions.add(new IgniteSqlZoneOption(key, option, s.end(this)));
      }
  )
}

SqlNumericLiteral UnsignedIntegerLiteral() :
{
}
{
    <UNSIGNED_INTEGER_LITERAL> {
        return SqlLiteral.createExactNumeric(token.image, getPos());
    }
}

SqlNodeList CreateZoneOptionList() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    final Span s = Span.of();
}
{
    CreateZoneOption(list)
    (
        <COMMA> { s.add(this); } CreateZoneOption(list)
    )*
    {
        return new SqlNodeList(list, s.end(this));
    }
}

void CreateZoneOption(List<SqlNode> list) :
{
    final Span s;
    final SqlIdentifier key;
    final SqlNode val;
}
{
    key = SimpleIdentifier() { s = span(); }
    <EQ>
    (
        <ALL> {
            list.add(new IgniteSqlZoneOption(key, IgniteSqlZoneOptionMode.ALL.symbol(getPos()), s.end(this)));
        }
    |
        val = Literal() {
            list.add(new IgniteSqlZoneOption(key, val, s.end(this)));
        }
    )
}

SqlDrop SqlDropZone(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier zoneId;
}
{
    <ZONE> ifExists = IfExistsOpt() zoneId = CompoundIdentifier() {
        return new IgniteSqlDropZone(s.end(this), ifExists, zoneId);
    }
}

SqlNode SqlAlterZone() :
{
    final Span s;
    final SqlIdentifier zoneId;
    final boolean ifExists;
    final SqlIdentifier newZoneId;
    SqlNodeList optionList = null;
}
{
    <ALTER> { s = span(); }
    <ZONE>
    ifExists = IfExistsOpt()
    zoneId = CompoundIdentifier()

    (
      <RENAME> <TO> newZoneId = SimpleIdentifier() {
        return new IgniteSqlAlterZoneRenameTo(s.end(this), zoneId, newZoneId, ifExists);
      }
      |
      <SET>
      (
        <DEFAULT_> {
          return new IgniteSqlAlterZoneSetDefault(s.end(this), zoneId, ifExists);
        }
        |
        LOOKAHEAD(2)
        { s.add(this); } optionList = AlterZoneOptions() {
          return new IgniteSqlAlterZoneSet(s.end(this), zoneId, optionList, ifExists);
        }
        |
        { s.add(this); } optionList = ZoneOptionsList() {
          return new IgniteSqlAlterZoneSet(s.end(this), zoneId, optionList, ifExists);
        }
        |
        optionList = ZoneOption()
        {
          return new IgniteSqlAlterZoneSet(s.end(this), zoneId, optionList, ifExists);
        }
      )
    )
}

SqlNodeList ZoneOption() :
{
    List<SqlNode> zoneOptions = new ArrayList<SqlNode>();

    final Span s = Span.of();
}
{
    ZoneElement(zoneOptions)
    {
        return new SqlNodeList(zoneOptions, s.end(this));
    }
}

SqlNodeList AlterZoneOptions() :
{
  List<SqlNode> list = new ArrayList<SqlNode>();
  final Span s = Span.of();
}
{
  AlterZoneOption(list)
  (
      <COMMA> { s.add(this); } AlterZoneOption(list)
  )*
  {
      return new SqlNodeList(list, s.end(this));
  }
}

void AlterZoneOption(List<SqlNode> list) :
{
    final Span s;
    final SqlIdentifier key;
    final SqlNode val;
}
{
    key = SimpleIdentifier() { s = span(); }
    <EQ>
    (
        <ALL> {
            list.add(new IgniteSqlZoneOption(key, IgniteSqlZoneOptionMode.ALL.symbol(getPos()), s.end(this)));
        }
    |
        val = Literal() {
            list.add(new IgniteSqlZoneOption(key, val, s.end(this)));
        }
    )
}

/**
* Parse datetime types: date, time, timestamp.
*
* TODO Method doesn't recognize TIME_WITH_LOCAL_TIME_ZONE, TIME_TZ and TIMESTAMP_TZ types and should be removed after IGNITE-21555.
*/
SqlTypeNameSpec IgniteDateTimeTypeName() :
{
    int precision = -1;
    SqlTypeName typeName;
    boolean withLocalTimeZone = false;
    final Span s;
}
{
    <DATE> {
        typeName = SqlTypeName.DATE;
        return new SqlBasicTypeNameSpec(typeName, getPos());
    }
|
    <TIME> { s = span(); }
    precision = PrecisionOpt()
    (<WITHOUT> <TIME> <ZONE>)?
    {
        typeName = SqlTypeName.TIME;
        return new SqlBasicTypeNameSpec(typeName, precision, s.end(this));
    }
|
    <TIMESTAMP> { s = span(); }
    precision = PrecisionOpt()
    typeName = TimestampZoneOpt()
    {
        return new SqlBasicTypeNameSpec(typeName, precision, s.end(this));
    }
}

SqlTypeName TimestampZoneOpt() :
{
}
{
    <WITHOUT> <TIME> <ZONE> { return SqlTypeName.TIMESTAMP; }
|
    <WITH> <LOCAL> <TIME> <ZONE> { return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE; }
|
    { return SqlTypeName.TIMESTAMP; }
}

SqlNode SqlStartTransaction() :
{
    final Span s;
    IgniteSqlStartTransactionMode mode = IgniteSqlStartTransactionMode.IMPLICIT_READ_WRITE;
}
{
    <START> { s = span(); }
    <TRANSACTION>
    [
        (
          LOOKAHEAD(2)
          <READ> <ONLY> {
            mode = IgniteSqlStartTransactionMode.READ_ONLY;
          }
          |
          <READ> <WRITE> {
            mode = IgniteSqlStartTransactionMode.READ_WRITE;
          }
        )
    ] {
       return new IgniteSqlStartTransaction(s.end(this), mode);
    }
}

SqlNode SqlCommitTransaction() :
{
    final Span s;
}
{
    <COMMIT> { s = span(); } {
       return new IgniteSqlCommitTransaction(s.end(this));
    }
}

SqlNode SqlKill() :
{
    final Span s;
    final SqlNode objectId;
    final IgniteSqlKillObjectType objectType;
    final Boolean noWait;
}
{
    <KILL> {
        s = span();
        objectType = SqlKillObjectType();
        objectId = StringLiteral();
        noWait = SqlKillNoWait();
    }
    {
        return new IgniteSqlKill(s.end(this), objectType, (SqlLiteral) objectId, noWait);
    }
}

IgniteSqlKillObjectType SqlKillObjectType():
{
    final IgniteSqlKillObjectType objectType;
}
{
    (
        <QUERY> { objectType = IgniteSqlKillObjectType.QUERY; }
        |
        <TRANSACTION> { objectType = IgniteSqlKillObjectType.TRANSACTION; }
        |
        <COMPUTE> { objectType = IgniteSqlKillObjectType.COMPUTE; }
    )
    {
        return objectType;
    }
}

Boolean SqlKillNoWait():
{
    Boolean noWait = null;
}
{
    (
        <NO> <WAIT> { noWait = true; }
    )?
    {
        return noWait;
    }
}

/**
 * Parses an EXPLAIN PLAN statement.
 */
SqlNode SqlIgniteExplain() :
{
    SqlNode stmt;
    Span s;
    IgniteSqlExplainMode mode = IgniteSqlExplainMode.PLAN;
}
{
    <EXPLAIN> { s = span(); }
    [ mode = ExplainMode() ]
    stmt = SqlQueryOrDml() {
        return new IgniteSqlExplain(s.end(this),
            stmt, mode.symbol(getPos()),
            nDynamicParams);
    }
}

IgniteSqlExplainMode ExplainMode() :
{
    IgniteSqlExplainMode mode = IgniteSqlExplainMode.PLAN;
}
{
    (
        <MAPPING> <FOR>
        {
            mode = IgniteSqlExplainMode.MAPPING;
        }
        |
        <PLAN> <FOR>
        {
        }
    )
    {
        return mode;
    }
}

/**
 * DESCRIBE is actually not supported, therefore let's throw an exception.
 */
SqlNode SqlIgniteDescribe() :
{ }
{
    <DESCRIBE> {
        throw SqlUtil.newContextException(span().pos(), IgniteResource.INSTANCE.unexpectedStatement(token.image));
    }
}
