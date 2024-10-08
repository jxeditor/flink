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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.legacy.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparison;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.ROW;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.STRUCTURED_TYPE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

/**
 * Utilities for checking {@link LogicalType} and avoiding a lot of type casting and repetitive
 * work.
 */
@Internal
public final class LogicalTypeChecks {

    private static final TimestampKindExtractor TIMESTAMP_KIND_EXTRACTOR =
            new TimestampKindExtractor();

    private static final LengthExtractor LENGTH_EXTRACTOR = new LengthExtractor();

    private static final PrecisionExtractor PRECISION_EXTRACTOR = new PrecisionExtractor();

    private static final ScaleExtractor SCALE_EXTRACTOR = new ScaleExtractor();

    private static final YearPrecisionExtractor YEAR_PRECISION_EXTRACTOR =
            new YearPrecisionExtractor();

    private static final DayPrecisionExtractor DAY_PRECISION_EXTRACTOR =
            new DayPrecisionExtractor();

    private static final FractionalPrecisionExtractor FRACTIONAL_PRECISION_EXTRACTOR =
            new FractionalPrecisionExtractor();

    private static final SingleFieldIntervalExtractor SINGLE_FIELD_INTERVAL_EXTRACTOR =
            new SingleFieldIntervalExtractor();

    private static final FieldCountExtractor FIELD_COUNT_EXTRACTOR = new FieldCountExtractor();

    private static final FieldNamesExtractor FIELD_NAMES_EXTRACTOR = new FieldNamesExtractor();

    /** Checks whether a (possibly nested) logical type fulfills the given predicate. */
    public static boolean hasNested(LogicalType logicalType, Predicate<LogicalType> predicate) {
        final NestedTypeSearcher typeSearcher = new NestedTypeSearcher(predicate);
        return logicalType.accept(typeSearcher).isPresent();
    }

    /**
     * Checks whether a (possibly nested) logical type contains {@link LegacyTypeInformationType} or
     * {@link TypeInformationRawType}.
     */
    public static boolean hasLegacyTypes(LogicalType logicalType) {
        return hasNested(logicalType, t -> t instanceof LegacyTypeInformationType);
    }

    public static boolean isTimeAttribute(LogicalType logicalType) {
        return isRowtimeAttribute(logicalType) || isProctimeAttribute(logicalType);
    }

    public static boolean isRowtimeAttribute(LogicalType logicalType) {
        return logicalType.isAnyOf(TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                && logicalType.accept(TIMESTAMP_KIND_EXTRACTOR) == TimestampKind.ROWTIME;
    }

    public static boolean isProctimeAttribute(LogicalType logicalType) {
        return logicalType.is(TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                && logicalType.accept(TIMESTAMP_KIND_EXTRACTOR) == TimestampKind.PROCTIME;
    }

    public static boolean canBeTimeAttributeType(LogicalType logicalType) {
        return logicalType.isAnyOf(TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    /**
     * Checks if the given type is a composite type.
     *
     * <p>Use {@link #getFieldCount(LogicalType)}, {@link #getFieldNames(LogicalType)}, {@link
     * #getFieldTypes(LogicalType)} for unified handling of composite types.
     *
     * @param logicalType Logical data type to check
     * @return True if the type is composite type.
     */
    public static boolean isCompositeType(LogicalType logicalType) {
        if (logicalType instanceof DistinctType) {
            return isCompositeType(((DistinctType) logicalType).getSourceType());
        }

        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        return typeRoot == STRUCTURED_TYPE || typeRoot == ROW;
    }

    public static int getLength(LogicalType logicalType) {
        return logicalType.accept(LENGTH_EXTRACTOR);
    }

    public static boolean hasLength(LogicalType logicalType, int length) {
        return getLength(logicalType) == length;
    }

    /** Returns the precision of all types that define a precision implicitly or explicitly. */
    public static int getPrecision(LogicalType logicalType) {
        return logicalType.accept(PRECISION_EXTRACTOR);
    }

    /** Checks the precision of a type that defines a precision implicitly or explicitly. */
    public static boolean hasPrecision(LogicalType logicalType, int precision) {
        return getPrecision(logicalType) == precision;
    }

    /** Returns the scale of all types that define a scale implicitly or explicitly. */
    public static int getScale(LogicalType logicalType) {
        return logicalType.accept(SCALE_EXTRACTOR);
    }

    /** Checks the scale of all types that define a scale implicitly or explicitly. */
    public static boolean hasScale(LogicalType logicalType, int scale) {
        return getScale(logicalType) == scale;
    }

    public static int getYearPrecision(LogicalType logicalType) {
        return logicalType.accept(YEAR_PRECISION_EXTRACTOR);
    }

    public static boolean hasYearPrecision(LogicalType logicalType, int yearPrecision) {
        return getYearPrecision(logicalType) == yearPrecision;
    }

    public static int getDayPrecision(LogicalType logicalType) {
        return logicalType.accept(DAY_PRECISION_EXTRACTOR);
    }

    public static boolean hasDayPrecision(LogicalType logicalType, int yearPrecision) {
        return getDayPrecision(logicalType) == yearPrecision;
    }

    public static int getFractionalPrecision(LogicalType logicalType) {
        return logicalType.accept(FRACTIONAL_PRECISION_EXTRACTOR);
    }

    public static boolean hasFractionalPrecision(LogicalType logicalType, int fractionalPrecision) {
        return getFractionalPrecision(logicalType) == fractionalPrecision;
    }

    public static boolean isSingleFieldInterval(LogicalType logicalType) {
        return logicalType.accept(SINGLE_FIELD_INTERVAL_EXTRACTOR);
    }

    /** Returns the field count of row and structured types. Other types return 1. */
    public static int getFieldCount(LogicalType logicalType) {
        return logicalType.accept(FIELD_COUNT_EXTRACTOR);
    }

    /** Returns the field names of row and structured types. */
    public static List<String> getFieldNames(LogicalType logicalType) {
        return logicalType.accept(FIELD_NAMES_EXTRACTOR);
    }

    /** Returns the field types of row and structured types. */
    public static List<LogicalType> getFieldTypes(LogicalType logicalType) {
        if (logicalType instanceof DistinctType) {
            return getFieldTypes(((DistinctType) logicalType).getSourceType());
        }
        return logicalType.getChildren();
    }

    /**
     * Checks whether the given {@link LogicalType} has a well-defined string representation when
     * calling {@link Object#toString()} on the internal data structure. The string representation
     * would be similar in SQL or in a programming language.
     *
     * <p>Note: This method might not be necessary anymore, once we have implemented a utility that
     * can convert any internal data structure to a well-defined string representation.
     */
    public static boolean hasWellDefinedString(LogicalType logicalType) {
        if (logicalType instanceof DistinctType) {
            return hasWellDefinedString(((DistinctType) logicalType).getSourceType());
        }
        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    public static boolean areComparable(
            LogicalType firstType,
            LogicalType secondType,
            StructuredComparison requiredComparison) {
        return areComparableWithNormalizedNullability(
                firstType.copy(true), secondType.copy(true), requiredComparison);
    }

    private static boolean areComparableWithNormalizedNullability(
            LogicalType firstType,
            LogicalType secondType,
            StructuredComparison requiredComparison) {
        // A hack to support legacy types. To be removed when we drop the legacy types.
        if (firstType instanceof LegacyTypeInformationType
                || secondType instanceof LegacyTypeInformationType) {
            return true;
        }

        // everything is comparable with null, it should return null in that case
        if (firstType.is(LogicalTypeRoot.NULL) || secondType.is(LogicalTypeRoot.NULL)) {
            return true;
        }

        if (firstType.getTypeRoot() == secondType.getTypeRoot()) {
            return areTypesOfSameRootComparable(firstType, secondType, requiredComparison);
        }

        if (firstType.is(LogicalTypeFamily.NUMERIC) && secondType.is(LogicalTypeFamily.NUMERIC)) {
            return true;
        }

        // DATE + ALL TIMESTAMPS
        if (firstType.is(LogicalTypeFamily.DATETIME) && secondType.is(LogicalTypeFamily.DATETIME)) {
            return true;
        }

        // VARCHAR + CHAR (we do not compare collations here)
        if (firstType.is(LogicalTypeFamily.CHARACTER_STRING)
                && secondType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            return true;
        }

        // VARBINARY + BINARY
        if (firstType.is(LogicalTypeFamily.BINARY_STRING)
                && secondType.is(LogicalTypeFamily.BINARY_STRING)) {
            return true;
        }

        return false;
    }

    private static boolean areTypesOfSameRootComparable(
            LogicalType firstType,
            LogicalType secondType,
            StructuredComparison requiredComparison) {
        switch (firstType.getTypeRoot()) {
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
                return areConstructedTypesComparable(firstType, secondType, requiredComparison);
            case DISTINCT_TYPE:
                return areDistinctTypesComparable(firstType, secondType, requiredComparison);
            case STRUCTURED_TYPE:
                return areStructuredTypesComparable(firstType, secondType, requiredComparison);
            case RAW:
                return areRawTypesComparable(firstType, secondType);
            default:
                return true;
        }
    }

    private static boolean areRawTypesComparable(LogicalType firstType, LogicalType secondType) {
        return firstType.equals(secondType)
                && Comparable.class.isAssignableFrom(
                        ((RawType<?>) firstType).getOriginatingClass());
    }

    private static boolean areDistinctTypesComparable(
            LogicalType firstType,
            LogicalType secondType,
            StructuredComparison requiredComparison) {
        DistinctType firstDistinctType = (DistinctType) firstType;
        DistinctType secondDistinctType = (DistinctType) secondType;
        return firstType.equals(secondType)
                && areComparable(
                        firstDistinctType.getSourceType(),
                        secondDistinctType.getSourceType(),
                        requiredComparison);
    }

    private static boolean areStructuredTypesComparable(
            LogicalType firstType,
            LogicalType secondType,
            StructuredComparison requiredComparison) {
        return firstType.equals(secondType)
                && hasRequiredComparison((StructuredType) firstType, requiredComparison);
    }

    private static boolean areConstructedTypesComparable(
            LogicalType firstType,
            LogicalType secondType,
            StructuredComparison requiredComparison) {
        List<LogicalType> firstChildren = firstType.getChildren();
        List<LogicalType> secondChildren = secondType.getChildren();

        if (firstChildren.size() != secondChildren.size()) {
            return false;
        }

        for (int i = 0; i < firstChildren.size(); i++) {
            if (!areComparable(firstChildren.get(i), secondChildren.get(i), requiredComparison)) {
                return false;
            }
        }

        return true;
    }

    private static Boolean hasRequiredComparison(
            StructuredType structuredType, StructuredComparison requiredComparison) {
        switch (requiredComparison) {
            case EQUALS:
                return structuredType.getComparison().isEquality();
            case FULL:
                return structuredType.getComparison().isComparison();
            case NONE:
            default:
                // this is not important, required comparison will never be NONE
                return true;
        }
    }

    private LogicalTypeChecks() {
        // no instantiation
    }

    // --------------------------------------------------------------------------------------------

    /** Extracts an attribute of logical types that define that attribute. */
    private static class Extractor<T> extends LogicalTypeDefaultVisitor<T> {
        @Override
        protected T defaultMethod(LogicalType logicalType) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid use of extractor %s. Called on logical type: %s",
                            this.getClass().getName(), logicalType));
        }
    }

    private static class LengthExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(CharType charType) {
            return charType.getLength();
        }

        @Override
        public Integer visit(VarCharType varCharType) {
            return varCharType.getLength();
        }

        @Override
        public Integer visit(BinaryType binaryType) {
            return binaryType.getLength();
        }

        @Override
        public Integer visit(VarBinaryType varBinaryType) {
            return varBinaryType.getLength();
        }
    }

    private static class PrecisionExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(DecimalType decimalType) {
            return decimalType.getPrecision();
        }

        @Override
        public Integer visit(TinyIntType tinyIntType) {
            return TinyIntType.PRECISION;
        }

        @Override
        public Integer visit(SmallIntType smallIntType) {
            return SmallIntType.PRECISION;
        }

        @Override
        public Integer visit(IntType intType) {
            return IntType.PRECISION;
        }

        @Override
        public Integer visit(BigIntType bigIntType) {
            return BigIntType.PRECISION;
        }

        @Override
        public Integer visit(FloatType floatType) {
            return FloatType.PRECISION;
        }

        @Override
        public Integer visit(DoubleType doubleType) {
            return DoubleType.PRECISION;
        }

        @Override
        public Integer visit(TimeType timeType) {
            return timeType.getPrecision();
        }

        @Override
        public Integer visit(TimestampType timestampType) {
            return timestampType.getPrecision();
        }

        @Override
        public Integer visit(ZonedTimestampType zonedTimestampType) {
            return zonedTimestampType.getPrecision();
        }

        @Override
        public Integer visit(LocalZonedTimestampType localZonedTimestampType) {
            return localZonedTimestampType.getPrecision();
        }
    }

    private static class ScaleExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(DecimalType decimalType) {
            return decimalType.getScale();
        }

        @Override
        public Integer visit(TinyIntType tinyIntType) {
            return 0;
        }

        @Override
        public Integer visit(SmallIntType smallIntType) {
            return 0;
        }

        @Override
        public Integer visit(IntType intType) {
            return 0;
        }

        @Override
        public Integer visit(BigIntType bigIntType) {
            return 0;
        }
    }

    private static class YearPrecisionExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(YearMonthIntervalType yearMonthIntervalType) {
            return yearMonthIntervalType.getYearPrecision();
        }
    }

    private static class DayPrecisionExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(DayTimeIntervalType dayTimeIntervalType) {
            return dayTimeIntervalType.getDayPrecision();
        }
    }

    private static class FractionalPrecisionExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(DayTimeIntervalType dayTimeIntervalType) {
            return dayTimeIntervalType.getFractionalPrecision();
        }
    }

    private static class TimestampKindExtractor extends Extractor<TimestampKind> {

        @Override
        public TimestampKind visit(TimestampType timestampType) {
            return timestampType.getKind();
        }

        @Override
        public TimestampKind visit(ZonedTimestampType zonedTimestampType) {
            return zonedTimestampType.getKind();
        }

        @Override
        public TimestampKind visit(LocalZonedTimestampType localZonedTimestampType) {
            return localZonedTimestampType.getKind();
        }
    }

    private static class SingleFieldIntervalExtractor extends Extractor<Boolean> {

        @Override
        public Boolean visit(YearMonthIntervalType yearMonthIntervalType) {
            switch (yearMonthIntervalType.getResolution()) {
                case YEAR:
                case MONTH:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public Boolean visit(DayTimeIntervalType dayTimeIntervalType) {
            switch (dayTimeIntervalType.getResolution()) {
                case DAY:
                case HOUR:
                case MINUTE:
                case SECOND:
                    return true;
                default:
                    return false;
            }
        }
    }

    private static class FieldCountExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(RowType rowType) {
            return rowType.getFieldCount();
        }

        @Override
        public Integer visit(StructuredType structuredType) {
            int fieldCount = 0;
            StructuredType currentType = structuredType;
            while (currentType != null) {
                fieldCount += currentType.getAttributes().size();
                currentType = currentType.getSuperType().orElse(null);
            }
            return fieldCount;
        }

        @Override
        public Integer visit(DistinctType distinctType) {
            return distinctType.getSourceType().accept(this);
        }

        @Override
        protected Integer defaultMethod(LogicalType logicalType) {
            // legacy
            if (logicalType.is(STRUCTURED_TYPE)) {
                return ((LegacyTypeInformationType<?>) logicalType).getTypeInformation().getArity();
            }
            return 1;
        }
    }

    private static class FieldNamesExtractor extends Extractor<List<String>> {

        @Override
        public List<String> visit(RowType rowType) {
            return rowType.getFieldNames();
        }

        @Override
        public List<String> visit(StructuredType structuredType) {
            final List<String> fieldNames = new ArrayList<>();
            // add super fields first
            structuredType
                    .getSuperType()
                    .map(superType -> superType.accept(this))
                    .ifPresent(fieldNames::addAll);
            // then specific fields
            structuredType.getAttributes().stream()
                    .map(StructuredAttribute::getName)
                    .forEach(fieldNames::add);
            return fieldNames;
        }

        @Override
        public List<String> visit(DistinctType distinctType) {
            return distinctType.getSourceType().accept(this);
        }

        @Override
        protected List<String> defaultMethod(LogicalType logicalType) {
            // legacy
            if (logicalType.is(STRUCTURED_TYPE)) {
                return Arrays.asList(
                        ((CompositeType<?>)
                                        ((LegacyTypeInformationType<?>) logicalType)
                                                .getTypeInformation())
                                .getFieldNames());
            }
            return super.defaultMethod(logicalType);
        }
    }

    /** Searches for a type (including children) satisfying the given predicate. */
    private static class NestedTypeSearcher
            extends LogicalTypeDefaultVisitor<Optional<LogicalType>> {

        private final Predicate<LogicalType> predicate;

        private NestedTypeSearcher(Predicate<LogicalType> predicate) {
            this.predicate = predicate;
        }

        @Override
        protected Optional<LogicalType> defaultMethod(LogicalType logicalType) {
            if (predicate.test(logicalType)) {
                return Optional.of(logicalType);
            }
            for (LogicalType child : logicalType.getChildren()) {
                final Optional<LogicalType> foundType = child.accept(this);
                if (foundType.isPresent()) {
                    return foundType;
                }
            }
            return Optional.empty();
        }
    }
}
