/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.amoro.parser

import java.util.Locale

import org.antlr.v4.runtime.ParserRuleContext
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.errors.QueryParsingErrors.{toSQLConf, toSQLId, toSQLStmt, toSQLType}
import org.apache.spark.sql.types.StringType

import org.apache.amoro.spark.sql.parser.MixedFormatSqlExtendParser._

/**
 * Object for grouping all error messages of the query parsing.
 * Currently it includes all ParseException.
 */
private[sql] object QueryParsingErrors extends QueryErrorsBase {

  def columnAliasInOperationNotAllowedError(op: String, ctx: TableAliasContext): Throwable = {
    new ParseException(s"Columns aliases are not allowed in $op.", ctx.identifierList())
  }

  def combinationQueryResultClausesUnsupportedError(ctx: QueryOrganizationContext): Throwable = {
    new ParseException(
      "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported",
      ctx)
  }

  def distributeByUnsupportedError(ctx: QueryOrganizationContext): Throwable = {
    new ParseException("DISTRIBUTE BY is not supported", ctx)
  }

  def transformNotSupportQuantifierError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.TRANSFORM_DISTINCT_ALL",
      messageParameters = Map.empty,
      ctx)
  }

  def transformWithSerdeUnsupportedError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.TRANSFORM_NON_HIVE",
      messageParameters = Map.empty,
      ctx)
  }

  def lateralWithPivotInFromClauseNotAllowedError(ctx: FromClauseContext): Throwable = {
    new ParseException("LATERAL cannot be used together with PIVOT in FROM clause", ctx)
  }

  def lateralJoinWithUsingJoinUnsupportedError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.LATERAL_JOIN_USING",
      messageParameters = Map.empty,
      ctx)
  }

  def unsupportedLateralJoinTypeError(ctx: ParserRuleContext, joinType: String): Throwable = {
    new ParseException(
      errorClass = "INVALID_LATERAL_JOIN_TYPE",
      messageParameters = Map("joinType" -> toSQLStmt(joinType)),
      ctx)
  }

  def invalidLateralJoinRelationError(ctx: RelationPrimaryContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.LATERAL_WITHOUT_SUBQUERY_OR_TABLE_VALUED_FUNC",
      ctx)
  }

  def repetitiveWindowDefinitionError(name: String, ctx: WindowClauseContext): Throwable = {
    new ParseException(
      "INVALID_SQL_SYNTAX.REPETITIVE_WINDOW_DEFINITION",
      Map("windowName" -> toSQLId(name)),
      ctx)
  }

  def invalidWindowReferenceError(name: String, ctx: WindowClauseContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.INVALID_WINDOW_REFERENCE",
      messageParameters = Map("windowName" -> toSQLId(name)),
      ctx)
  }

  def cannotResolveWindowReferenceError(name: String, ctx: WindowClauseContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.UNRESOLVED_WINDOW_REFERENCE",
      messageParameters = Map("windowName" -> toSQLId(name)),
      ctx)
  }

  def incompatibleJoinTypesError(
      joinType1: String,
      joinType2: String,
      ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INCOMPATIBLE_JOIN_TYPES",
      messageParameters = Map(
        "joinType1" -> joinType1.toUpperCase(Locale.ROOT),
        "joinType2" -> joinType2.toUpperCase(Locale.ROOT)),
      ctx = ctx)
  }

  def emptyInputForTableSampleError(ctx: ParserRuleContext): Throwable = {
    new ParseException("TABLESAMPLE does not accept empty inputs.", ctx)
  }

  def tableSampleByBytesUnsupportedError(msg: String, ctx: SampleMethodContext): Throwable = {
    new ParseException(s"TABLESAMPLE($msg) is not supported", ctx)
  }

  def invalidByteLengthLiteralError(bytesStr: String, ctx: SampleByBytesContext): Throwable = {
    new ParseException(
      s"$bytesStr is not a valid byte length literal, " +
        "expected syntax: DIGIT+ ('B' | 'K' | 'M' | 'G')",
      ctx)
  }

  def invalidEscapeStringError(ctx: PredicateContext): Throwable = {
    new ParseException("Invalid escape string. Escape string must contain only one character.", ctx)
  }

  def trimOptionUnsupportedError(trimOption: Int, ctx: TrimContext): Throwable = {
    new ParseException(
      "Function trim doesn't support with " +
        s"type $trimOption. Please use BOTH, LEADING or TRAILING as trim type",
      ctx)
  }

  def functionNameUnsupportedError(functionName: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(s"Unsupported function name '$functionName'", ctx)
  }

  def cannotParseValueTypeError(
      valueType: String,
      value: String,
      ctx: TypeConstructorContext): Throwable = {
    new ParseException(s"Cannot parse the $valueType value: $value", ctx)
  }

  def cannotParseIntervalValueError(value: String, ctx: TypeConstructorContext): Throwable = {
    new ParseException(s"Cannot parse the INTERVAL value: $value", ctx)
  }

  def literalValueTypeUnsupportedError(
      valueType: String,
      ctx: TypeConstructorContext): Throwable = {
    new ParseException(s"Literals of type '$valueType' are currently not supported.", ctx)
  }

  def parsingValueTypeError(
      e: IllegalArgumentException,
      valueType: String,
      ctx: TypeConstructorContext): Throwable = {
    val message = Option(e.getMessage).getOrElse(s"Exception parsing $valueType")
    new ParseException(message, ctx)
  }

  def invalidNumericLiteralRangeError(
      rawStrippedQualifier: String,
      minValue: BigDecimal,
      maxValue: BigDecimal,
      typeName: String,
      ctx: NumberContext): Throwable = {
    new ParseException(
      s"Numeric literal $rawStrippedQualifier does not " +
        s"fit in range [$minValue, $maxValue] for type $typeName",
      ctx)
  }

  def moreThanOneFromToUnitInIntervalLiteralError(ctx: ParserRuleContext): Throwable = {
    new ParseException("Can only have a single from-to unit in the interval literal syntax", ctx)
  }

  def invalidIntervalLiteralError(ctx: IntervalContext): Throwable = {
    new ParseException("at least one time unit should be given for interval literal", ctx)
  }

  def invalidIntervalFormError(value: String, ctx: MultiUnitsIntervalContext): Throwable = {
    new ParseException(
      "Can only use numbers in the interval value part for" +
        s" multiple unit value pairs interval form, but got invalid value: $value",
      ctx)
  }

  def invalidFromToUnitValueError(ctx: IntervalValueContext): Throwable = {
    new ParseException("The value of from-to unit must be a string", ctx)
  }

  def fromToIntervalUnsupportedError(
      from: String,
      to: String,
      ctx: ParserRuleContext): Throwable = {
    new ParseException(s"Intervals FROM $from TO $to are not supported.", ctx)
  }

  def mixedIntervalUnitsError(literal: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(s"Cannot mix year-month and day-time fields: $literal", ctx)
  }

  def dataTypeUnsupportedError(dataType: String, ctx: PrimitiveDataTypeContext): Throwable = {
    new ParseException(s"DataType $dataType is not supported.", ctx)
  }

  def charTypeMissingLengthError(dataType: String, ctx: PrimitiveDataTypeContext): Throwable = {
    new ParseException(
      errorClass = "DATATYPE_MISSING_SIZE",
      messageParameters = Map("type" -> toSQLType(dataType)),
      ctx)
  }

  def partitionTransformNotExpectedError(
      name: String,
      describe: String,
      ctx: ApplyTransformContext): Throwable = {
    new ParseException(s"Expected a column reference for transform $name: $describe", ctx)
  }

  def wrongNumberArgumentsForTransformError(
      name: String,
      actualNum: Int,
      ctx: ApplyTransformContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.TRANSFORM_WRONG_NUM_ARGS",
      messageParameters = Map(
        "transform" -> toSQLId(name),
        "expectedNum" -> "1",
        "actualNum" -> actualNum.toString),
      ctx)
  }

  def invalidBucketsNumberError(describe: String, ctx: ApplyTransformContext): Throwable = {
    new ParseException(s"Invalid number of buckets: $describe", ctx)
  }

  def cannotCleanReservedNamespacePropertyError(
      property: String,
      ctx: ParserRuleContext,
      msg: String): ParseException = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY",
      messageParameters = Map("property" -> property, "msg" -> msg),
      ctx)
  }

  def cannotCleanReservedTablePropertyError(
      property: String,
      ctx: ParserRuleContext,
      msg: String): ParseException = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY",
      messageParameters = Map("property" -> property, "msg" -> msg),
      ctx)
  }

  def duplicatedTablePathsFoundError(
      pathOne: String,
      pathTwo: String,
      ctx: ParserRuleContext): Throwable = {
    new ParseException(
      s"Duplicated table paths found: '$pathOne' and '$pathTwo'. LOCATION" +
        s" and the case insensitive key 'path' in OPTIONS are all used to indicate the custom" +
        s" table path, you can only specify one of them.",
      ctx)
  }

  def storedAsAndStoredByBothSpecifiedError(ctx: CreateFileFormatContext): Throwable = {
    new ParseException("Expected either STORED AS or STORED BY, not both", ctx)
  }

  def operationInHiveStyleCommandUnsupportedError(
      operation: String,
      command: String,
      ctx: StatementContext,
      msgOpt: Option[String] = None): Throwable = {
    val basicError = s"$operation is not supported in Hive-style $command"
    val msg = if (msgOpt.isDefined) {
      s"$basicError, ${msgOpt.get}."
    } else {
      basicError
    }
    new ParseException(msg, ctx)
  }

  def operationNotAllowedError(message: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(s"Operation not allowed: $message", ctx)
  }

  def computeStatisticsNotExpectedError(ctx: IdentifierContext): Throwable = {
    new ParseException(s"Expected `NOSCAN` instead of `${ctx.getText}`", ctx)
  }

  def showFunctionsUnsupportedError(identifier: String, ctx: IdentifierContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.SHOW_FUNCTIONS_INVALID_SCOPE",
      messageParameters = Map("scope" -> toSQLId(identifier)),
      ctx)
  }

  def showFunctionsInvalidPatternError(pattern: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.SHOW_FUNCTIONS_INVALID_PATTERN",
      messageParameters = Map("pattern" -> toSQLId(pattern)),
      ctx)
  }

  def duplicateCteDefinitionNamesError(duplicateNames: String, ctx: CtesContext): Throwable = {
    new ParseException(s"CTE definition can't have duplicate names: $duplicateNames.", ctx)
  }

  def sqlStatementUnsupportedError(sqlText: String, position: Origin): Throwable = {
    new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
  }

  def unquotedIdentifierError(ident: String, ctx: ErrorIdentContext): Throwable = {
    new ParseException(
      s"Possibly unquoted identifier $ident detected. " +
        s"Please consider quoting it with back-quotes as `$ident`",
      ctx)
  }

  def duplicateClausesError(clauseName: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(s"Found duplicate clauses: $clauseName", ctx)
  }

  def duplicateKeysError(key: String, ctx: ParserRuleContext): Throwable = {
    // Found duplicate keys '$key'
    new ParseException(
      errorClass = "DUPLICATE_KEY",
      messageParameters = Map("keyColumn" -> toSQLId(key)),
      ctx)
  }

  def unexpectedFomatForSetConfigurationError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      s"""
         |Expected format is 'SET', 'SET key', or 'SET key=value'. If you want to include
         |special characters in key, or include semicolon in value, please use quotes,
         |e.g., SET `ke y`=`v;alue`.
       """.stripMargin.replaceAll("\n", " "),
      ctx)
  }

  def invalidPropertyKeyForSetQuotedConfigurationError(
      keyCandidate: String,
      valueStr: String,
      ctx: ParserRuleContext): ParseException = {
    new ParseException(
      errorClass = "INVALID_PROPERTY_KEY",
      messageParameters = Map(
        "key" -> toSQLConf(keyCandidate),
        "value" -> toSQLConf(valueStr)),
      ctx)
  }

  def invalidPropertyValueForSetQuotedConfigurationError(
      valueCandidate: String,
      keyStr: String,
      ctx: ParserRuleContext): ParseException = {
    new ParseException(
      errorClass = "INVALID_PROPERTY_VALUE",
      messageParameters = Map(
        "value" -> toSQLConf(valueCandidate),
        "key" -> toSQLConf(keyStr)),
      ctx)
  }

  def intervalValueOutOfRangeError(ctx: IntervalContext): Throwable = {
    new ParseException(
      "The interval value must be in the range of [-18, +18] hours" +
        " with second precision",
      ctx)
  }

  def useDefinedRecordReaderOrWriterClassesError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      "Unsupported operation: Used defined record reader/writer classes.",
      ctx)
  }

  def invalidGroupingSetError(element: String, ctx: GroupingAnalyticsContext): Throwable = {
    new ParseException(s"Empty set in $element grouping sets is not supported.", ctx)
  }

  def invalidTableValuedFunctionNameError(
      name: Seq[String],
      ctx: TableValuedFunctionContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.INVALID_TABLE_VALUED_FUNC_NAME",
      messageParameters = Map("funcName" -> toSQLId(name)),
      ctx)
  }

  def unclosedBracketedCommentError(command: String, position: Origin): Throwable = {
    new ParseException(Some(command), "Unclosed bracketed comment", position, position)
  }

  def invalidTimeTravelSpec(reason: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(s"Invalid time travel spec: $reason.", ctx)
  }

  def invalidNameForDropTempFunc(name: Seq[String], ctx: ParserRuleContext): Throwable = {
    new ParseException(
      s"DROP TEMPORARY FUNCTION requires a single part name but got: ${name.quoted}",
      ctx)
  }
}
