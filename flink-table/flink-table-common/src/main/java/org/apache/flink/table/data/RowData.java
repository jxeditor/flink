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

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.Variant;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldCount;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;

/**
 * Base interface for an internal data structure representing data of {@link RowType} and other
 * (possibly nested) structured types such as {@link StructuredType} in the table ecosystem.
 *
 * <p>All top-level records that are travelling through Table API or SQL pipelines during runtime
 * are instances of this interface. Each {@link RowData} contains a {@link RowKind} which represents
 * the kind of change that a row describes in a changelog. The {@link RowKind} is just metadata
 * information of row and thus not part of the table's schema, i.e., not a dedicated field.
 *
 * <p>Note: All fields of this data structure must be internal data structures.
 *
 * <p>The {@link RowData} interface has different implementations which are designed for different
 * scenarios:
 *
 * <ul>
 *   <li>The binary-oriented implementation {@code BinaryRowData} is backed by references to {@link
 *       MemorySegment} instead of using Java objects to reduce the serialization/deserialization
 *       overhead.
 *   <li>The object-oriented implementation {@link GenericRowData} is backed by an array of Java
 *       {@link Object} which is easy to construct and efficient to update.
 * </ul>
 *
 * <p>{@link GenericRowData} is intended for public use and has stable behavior. It is recommended
 * to construct instances of {@link RowData} with this class if internal data structures are
 * required.
 *
 * <p>The mappings from Flink's Table API and SQL data types to the internal data structures are
 * listed in the following table:
 *
 * <pre>
 * +--------------------------------+-----------------------------------------+
 * | SQL Data Types                 | Internal Data Structures                |
 * +--------------------------------+-----------------------------------------+
 * | BOOLEAN                        | boolean                                 |
 * +--------------------------------+-----------------------------------------+
 * | CHAR / VARCHAR / STRING        | {@link StringData}                      |
 * +--------------------------------+-----------------------------------------+
 * | BINARY / VARBINARY / BYTES     | byte[]                                  |
 * +--------------------------------+-----------------------------------------+
 * | DECIMAL                        | {@link DecimalData}                     |
 * +--------------------------------+-----------------------------------------+
 * | TINYINT                        | byte                                    |
 * +--------------------------------+-----------------------------------------+
 * | SMALLINT                       | short                                   |
 * +--------------------------------+-----------------------------------------+
 * | INT                            | int                                     |
 * +--------------------------------+-----------------------------------------+
 * | BIGINT                         | long                                    |
 * +--------------------------------+-----------------------------------------+
 * | FLOAT                          | float                                   |
 * +--------------------------------+-----------------------------------------+
 * | DOUBLE                         | double                                  |
 * +--------------------------------+-----------------------------------------+
 * | DATE                           | int (number of days since epoch)        |
 * +--------------------------------+-----------------------------------------+
 * | TIME                           | int (number of milliseconds of the day) |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP                      | {@link TimestampData}                   |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP WITH LOCAL TIME ZONE | {@link TimestampData}                   |
 * +--------------------------------+-----------------------------------------+
 * | INTERVAL YEAR TO MONTH         | int (number of months)                  |
 * +--------------------------------+-----------------------------------------+
 * | INTERVAL DAY TO MONTH          | long (number of milliseconds)           |
 * +--------------------------------+-----------------------------------------+
 * | ROW / structured types         | {@link RowData}                         |
 * +--------------------------------+-----------------------------------------+
 * | ARRAY                          | {@link ArrayData}                       |
 * +--------------------------------+-----------------------------------------+
 * | MAP / MULTISET                 | {@link MapData}                         |
 * +--------------------------------+-----------------------------------------+
 * | RAW                            | {@link RawValueData}                    |
 * +--------------------------------+-----------------------------------------+
 * </pre>
 *
 * <p>Nullability is always handled by the container data structure.
 */
@PublicEvolving
public interface RowData {

    /**
     * Returns the number of fields in this row.
     *
     * <p>The number does not include {@link RowKind}. It is kept separately.
     */
    int getArity();

    /**
     * Returns the kind of change that this row describes in a changelog.
     *
     * @see RowKind
     */
    RowKind getRowKind();

    /**
     * Sets the kind of change that this row describes in a changelog.
     *
     * @see RowKind
     */
    void setRowKind(RowKind kind);

    // ------------------------------------------------------------------------------------------
    // Read-only accessor methods
    // ------------------------------------------------------------------------------------------

    /** Returns true if the field is null at the given position. */
    boolean isNullAt(int pos);

    /** Returns the boolean value at the given position. */
    boolean getBoolean(int pos);

    /** Returns the byte value at the given position. */
    byte getByte(int pos);

    /** Returns the short value at the given position. */
    short getShort(int pos);

    /** Returns the integer value at the given position. */
    int getInt(int pos);

    /** Returns the long value at the given position. */
    long getLong(int pos);

    /** Returns the float value at the given position. */
    float getFloat(int pos);

    /** Returns the double value at the given position. */
    double getDouble(int pos);

    /** Returns the string value at the given position. */
    StringData getString(int pos);

    /**
     * Returns the decimal value at the given position.
     *
     * <p>The precision and scale are required to determine whether the decimal value was stored in
     * a compact representation (see {@link DecimalData}).
     */
    DecimalData getDecimal(int pos, int precision, int scale);

    /**
     * Returns the timestamp value at the given position.
     *
     * <p>The precision is required to determine whether the timestamp value was stored in a compact
     * representation (see {@link TimestampData}).
     */
    TimestampData getTimestamp(int pos, int precision);

    /** Returns the raw value at the given position. */
    <T> RawValueData<T> getRawValue(int pos);

    /** Returns the binary value at the given position. */
    byte[] getBinary(int pos);

    /** Returns the array value at the given position. */
    ArrayData getArray(int pos);

    /** Returns the map value at the given position. */
    MapData getMap(int pos);

    /**
     * Returns the row value at the given position.
     *
     * <p>The number of fields is required to correctly extract the row.
     */
    RowData getRow(int pos, int numFields);

    /** Returns the variant value at the given position. */
    Variant getVariant(int pos);

    // ------------------------------------------------------------------------------------------
    // Access Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an accessor for getting elements in an internal row data structure at the given
     * position.
     *
     * @param fieldType the element type of the row
     * @param fieldPos the element position of the row
     */
    static FieldGetter createFieldGetter(LogicalType fieldType, int fieldPos) {
        final FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter = row -> row.getString(fieldPos);
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = row -> row.getBinary(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter = row -> row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldGetter = row -> row.getTimestamp(fieldPos, timestampPrecision);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                fieldGetter = row -> row.getArray(fieldPos);
                break;
            case MULTISET:
            case MAP:
                fieldGetter = row -> row.getMap(fieldPos);
                break;
            case ROW:
            case STRUCTURED_TYPE:
                final int rowFieldCount = getFieldCount(fieldType);
                fieldGetter = row -> row.getRow(fieldPos, rowFieldCount);
                break;
            case DISTINCT_TYPE:
                fieldGetter =
                        createFieldGetter(((DistinctType) fieldType).getSourceType(), fieldPos);
                break;
            case RAW:
                fieldGetter = row -> row.getRawValue(fieldPos);
                break;
            case VARIANT:
                fieldGetter = row -> row.getVariant(fieldPos);
                break;
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            case DESCRIPTOR:
            default:
                throw new IllegalArgumentException();
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }

    /**
     * Accessor for getting the field of a row during runtime.
     *
     * @see #createFieldGetter(LogicalType, int)
     */
    @PublicEvolving
    interface FieldGetter extends Serializable {
        /**
         * Converters and serializers always support nullability. The NOT NULL constraint is only
         * considered on SQL semantic level but not data transfer. E.g. partial deletes (i.e.
         * key-only upserts) set all non-key fields to null, regardless of logical type.
         */
        @Nullable
        Object getFieldOrNull(RowData row);
    }
}
