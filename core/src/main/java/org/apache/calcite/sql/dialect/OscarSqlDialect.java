package org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * @Auther: chy
 * @Date: 2021/8/20 16:12
 * @Description:
 */
public class OscarSqlDialect extends SqlDialect {

    /** MySQL type system. */
    public static final RelDataTypeSystem OSCAR_TYPE_SYSTEM =
            new RelDataTypeSystemImpl() {
                @Override
                public int getMaxPrecision(SqlTypeName typeName) {
                    switch (typeName) {
                        case CHAR:
                            return 255;
                        case VARCHAR:
                            return 8000;
                        case TIMESTAMP:
                            return 6;
                        default:
                            return super.getMaxPrecision(typeName);
                    }
                }
            };

    public OscarSqlDialect(Context context) {
        super(context);
    }

    //返回方言是否支持字符集名称作为数据类型的一部分，例如 {@code VARCHAR(30) CHARACTER SET `ISO-8859-1`}。
    @Override
    public boolean supportsCharSet() {
        return false;
    }

    //FROM 子句中的子查询是否必须有别名
    @Override
    public boolean requiresAliasForFromItems() {
        return true;
    }

    public boolean supportsAliasedValues() {
        // MySQL supports VALUES only in INSERT; not in a FROM clause
        return false;
    }

    @Override
    public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
                                   SqlNode fetch) {
        unparseFetchUsingLimit(writer, offset, fetch);
    }

    @Override
    public SqlNode emulateNullDirection(SqlNode node,
                                        boolean nullsFirst, boolean desc) {
        return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
    }

    //是否支持聚合嵌套SELECT SUM(SUM(1))
    @Override
    public boolean supportsNestedAggregations() {
        return false;
    }

    //返回此方言是否支持“GROUP BY”中的“WITH ROLLUP”
    @Override
    public boolean supportsGroupByWithRollup() {
        return false;
    }

    @Override
    public CalendarPolicy getCalendarPolicy() {
        return CalendarPolicy.SHIFT;
    }

    @Override
    public SqlNode rewriteSingleValueExpr(SqlNode aggCall) {
        final SqlNode operand = ((SqlBasicCall) aggCall).operand(0);
        final SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
      final SqlNode unionOperand = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY,
          SqlNodeList.of(nullLiteral), null, null, null, null,
          SqlNodeList.EMPTY, null, null, null, SqlNodeList.EMPTY);
        // For MySQL, generate
        //   CASE COUNT(*)
        //   WHEN 0 THEN NULL
        //   WHEN 1 THEN <result>
        //   ELSE (SELECT NULL UNION ALL SELECT NULL)
        //   END
        final SqlNode caseExpr =
                new SqlCase(SqlParserPos.ZERO,
                        SqlStdOperatorTable.COUNT.createCall(SqlParserPos.ZERO, operand),
                        SqlNodeList.of(
                                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
                                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)),
                        SqlNodeList.of(
                                nullLiteral,
                                operand),
                        SqlStdOperatorTable.SCALAR_QUERY.createCall(SqlParserPos.ZERO,
                                SqlStdOperatorTable.UNION_ALL
                                        .createCall(SqlParserPos.ZERO, unionOperand, unionOperand)));

        LOGGER.debug("SINGLE_VALUE rewritten into [{}]", caseExpr);

        return caseExpr;
    }

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call,
                            int leftPrec, int rightPrec) {
        switch (call.getKind()) {
            case FLOOR:
                if (call.operandCount() != 2) {
                    super.unparseCall(writer, call, leftPrec, rightPrec);
                    return;
                }

                unparseFloor(writer, call);
                break;

            default:
                super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }

    private void unparseFloor(SqlWriter writer, SqlCall call) {
        SqlLiteral node = call.operand(1);
        TimeUnitRange unit = (TimeUnitRange) node.getValue();

        if (unit == TimeUnitRange.WEEK) {
            writer.print("STR_TO_DATE");
            SqlWriter.Frame frame = writer.startList("(", ")");

            writer.print("DATE_FORMAT(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(", '%x%v-1'), '%x%v-%w'");
            writer.endList(frame);
            return;
        }

        String format;
        switch (unit) {
            case YEAR:
                format = "%Y-01-01";
                break;
            case MONTH:
                format = "%Y-%m-01";
                break;
            case DAY:
                format = "%Y-%m-%d";
                break;
            case HOUR:
                format = "%Y-%m-%d %H:00:00";
                break;
            case MINUTE:
                format = "%Y-%m-%d %H:%i:00";
                break;
            case SECOND:
                format = "%Y-%m-%d %H:%i:%s";
                break;
            default:
                throw new AssertionError("MYSQL does not support FLOOR for time unit: "
                        + unit);
        }

        writer.print("DATE_FORMAT");
        SqlWriter.Frame frame = writer.startList("(", ")");
        call.operand(0).unparse(writer, 0, 0);
        writer.sep(",", true);
        writer.print("'" + format + "'");
        writer.endList(frame);
    }

    @Override
    public void unparseSqlIntervalQualifier(SqlWriter writer,
                                            SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem) {

        //  Unit Value         | Expected Format
        // --------------------+-------------------------------------------
        //  MICROSECOND        | MICROSECONDS
        //  SECOND             | SECONDS
        //  MINUTE             | MINUTES
        //  HOUR               | HOURS
        //  DAY                | DAYS
        //  WEEK               | WEEKS
        //  MONTH              | MONTHS
        //  QUARTER            | QUARTERS
        //  YEAR               | YEARS
        //  MINUTE_SECOND      | 'MINUTES:SECONDS'
        //  HOUR_MINUTE        | 'HOURS:MINUTES'
        //  DAY_HOUR           | 'DAYS HOURS'
        //  YEAR_MONTH         | 'YEARS-MONTHS'
        //  MINUTE_MICROSECOND | 'MINUTES:SECONDS.MICROSECONDS'
        //  HOUR_MICROSECOND   | 'HOURS:MINUTES:SECONDS.MICROSECONDS'
        //  SECOND_MICROSECOND | 'SECONDS.MICROSECONDS'
        //  DAY_MINUTE         | 'DAYS HOURS:MINUTES'
        //  DAY_MICROSECOND    | 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
        //  DAY_SECOND         | 'DAYS HOURS:MINUTES:SECONDS'
        //  HOUR_SECOND        | 'HOURS:MINUTES:SECONDS'

        if (!qualifier.useDefaultFractionalSecondPrecision()) {
            throw new AssertionError("Fractional second precision is not supported now ");
        }

        final String start = validate(qualifier.timeUnitRange.startUnit).name();
        if (qualifier.timeUnitRange.startUnit == TimeUnit.SECOND
                || qualifier.timeUnitRange.endUnit == null) {
            writer.keyword(start);
        } else {
            writer.keyword(start + "_" + qualifier.timeUnitRange.endUnit.name());
        }
    }

    private TimeUnit validate(TimeUnit timeUnit) {
        switch (timeUnit) {
            case MICROSECOND:
            case SECOND:
            case MINUTE:
            case HOUR:
            case DAY:
            case WEEK:
            case MONTH:
            case QUARTER:
            case YEAR:
                return timeUnit;
            default:
                throw new AssertionError(" Time unit " + timeUnit + "is not supported now.");
        }
    }

}
