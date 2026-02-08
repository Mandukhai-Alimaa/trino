// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package trino

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	// Trino's default maximum query size limit (1 million bytes)
	TrinoMaxQuerySizeBytes = 1_000_000
)

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) GetCurrentCatalog() (string, error) {
	var catalog string
	err := c.Db.QueryRowContext(context.Background(), "SELECT current_catalog").Scan(&catalog)
	if err != nil {
		return "", c.ErrorHelper.WrapIO(err, "failed to get current catalog")
	}
	return catalog, nil
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) GetCurrentDbSchema() (string, error) {
	var schema string
	err := c.Db.QueryRowContext(context.Background(), "SELECT current_schema").Scan(&schema)
	if err != nil {
		return "", c.ErrorHelper.WrapIO(err, "failed to get current schema")
	}
	return schema, nil
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) SetCurrentCatalog(catalog string) error {
	if catalog == "" {
		return nil // No-op for empty catalog
	}
	_, err := c.Db.ExecContext(context.Background(), "USE "+quoteIdentifier(catalog)+".information_schema")
	return err
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) SetCurrentDbSchema(schema string) error {
	if schema == "" {
		return nil // No-op for empty schema
	}
	_, err := c.Db.ExecContext(context.Background(), "USE "+quoteIdentifier(schema))
	return err
}

func (c *trinoConnectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if c.version == "" {
		var version string
		if err := c.Conn.QueryRowContext(ctx, "SELECT node_version FROM system.runtime.nodes LIMIT 1").Scan(&version); err != nil {
			return c.ErrorHelper.WrapIO(err, "failed to get version")
		}
		c.version = fmt.Sprintf("Trino %s", version)
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, c.version)
}

// GetTableSchema returns the Arrow schema for a Trino table
func (c *trinoConnectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (schema *arrow.Schema, err error) {
	var catalogName, schemaName string

	// Get catalog
	if catalog != nil && *catalog != "" {
		catalogName = *catalog
	} else {
		catalogName, err = c.GetCurrentCatalog()
		if err != nil {
			return nil, err
		}
	}

	// Get schema
	if dbSchema != nil && *dbSchema != "" {
		schemaName = *dbSchema
	} else {
		schemaName, err = c.GetCurrentDbSchema()
		if err != nil {
			return nil, err
		}
	}

	qualifiedTableName := fmt.Sprintf("%s.%s.%s", quoteIdentifier(catalogName), quoteIdentifier(schemaName), quoteIdentifier(tableName))

	query := fmt.Sprintf("SELECT * FROM %s WHERE 1=0", qualifiedTableName)
	stmt, err := c.Conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to prepare statement")
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	// Go's database/sql package doesn't provide a direct way to extract
	// column types from a prepared statement without executing it.
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to execute schema query")
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	// Get column types from the result set
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, c.ErrorHelper.WrapInternal(err, "failed to get column types")
	}

	if len(columnTypes) == 0 {
		return nil, c.ErrorHelper.NotFound("table not found: %s", tableName)
	}

	// Convert column types to Arrow fields using the existing type converter
	fields := make([]arrow.Field, len(columnTypes))
	for i, colType := range columnTypes {
		wrappedColType := sqlwrapper.ColumnType{
			Name:             colType.Name(),
			DatabaseTypeName: colType.DatabaseTypeName(),
			Nullable:         true, // Assume every column in always nullable since trino go client does not provide clean way to get nullability.
		}

		// Add precision and scale if available
		if precision, scale, ok := colType.DecimalSize(); ok {
			p, s := int64(precision), int64(scale)
			wrappedColType.Precision = &p
			wrappedColType.Scale = &s
		} else if length, ok := colType.Length(); ok {
			l := int64(length)
			wrappedColType.Precision = &l
		}

		arrowType, nullable, metadata, err := c.TypeConverter.ConvertRawColumnType(wrappedColType)
		if err != nil {
			return nil, c.ErrorHelper.WrapInternal(err, "failed to convert column type for %s", colType.Name())
		}

		fields[i] = arrow.Field{
			Name:     colType.Name(),
			Type:     arrowType,
			Nullable: nullable,
			Metadata: metadata,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// QuoteIdentifier implements BulkIngester
func (c *trinoConnectionImpl) QuoteIdentifier(name string) string {
	return quoteIdentifier(name)
}

// GetPlaceholder implements BulkIngester
func (c *trinoConnectionImpl) GetPlaceholder(field *arrow.Field, index int) string {
	return c.getParameterPlaceholder(*field)
}

// Ensure trinoConnectionImpl implements BulkIngester
var _ sqlwrapper.BulkIngester = (*trinoConnectionImpl)(nil)

// ExecuteBulkIngest performs Trino bulk ingest using batched INSERT statements.
func (c *trinoConnectionImpl) ExecuteBulkIngest(ctx context.Context, conn *sqlwrapper.LoggingConn, options *driverbase.BulkIngestOptions, stream array.RecordReader) (rowCount int64, err error) {
	schema := stream.Schema()
	if err := c.createTableIfNeeded(ctx, conn, options.TableName, schema, options); err != nil {
		return -1, c.ErrorHelper.WrapIO(err, "failed to create table")
	}

	// Determine batch size
	if options.IngestBatchSize <= 0 {
		maxQuerySizeBytes := TrinoMaxQuerySizeBytes
		if options.MaxQuerySizeBytes > 0 {
			maxQuerySizeBytes = options.MaxQuerySizeBytes
		}
		batchSize := c.calculateFixedBatchSizeFromSchema(schema, options.TableName, maxQuerySizeBytes)
		options.IngestBatchSize = batchSize
	}

	// Use standard batched bulk ingest with calculated batch size or user provided batch size
	return sqlwrapper.ExecuteBatchedBulkIngest(
		ctx, conn, options, stream,
		c.TypeConverter, c, &c.Base().ErrorHelper,
	)
}

// calculateFixedBatchSizeFromSchema determines optimal fixed batch size
// based on worst-case serialization estimates from Arrow schema.
func (c *trinoConnectionImpl) calculateFixedBatchSizeFromSchema(
	schema *arrow.Schema,
	tableName string,
	maxPayloadSize int,
) int {
	var worstCaseRowSize int

	for _, field := range schema.Fields() {
		worstCaseRowSize += c.estimateWorstCaseSerializedSize(field)
		worstCaseRowSize += 2 // ", " separator between columns
	}

	// Row delimiters: "()" for first row, ", ()" for subsequent
	worstCaseRowSize += 4

	baseOverhead := len(fmt.Sprintf("INSERT INTO %s VALUES ", quoteIdentifier(tableName)))

	availableSpace := maxPayloadSize - baseOverhead
	batchSize := max(availableSpace/worstCaseRowSize, 1)

	return batchSize
}

// estimateWorstCaseSerializedSize calculates the worst-case serialized size
// for an Arrow field when converted to Trino SQL format using trino.Serial().
//
// This matches the exact serialization logic in https://github.com/trinodb/trino-go-client/blob/master/trino/serial.go#L101
// to provide accurate worst-case estimates without actually serializing data.
func (c *trinoConnectionImpl) estimateWorstCaseSerializedSize(field arrow.Field) int {
	dataType := field.Type
	var dataSize int

	// Calculate placeholder overhead: "CAST(? AS REAL)" vs "?"
	placeholder := c.getParameterPlaceholder(field)
	placeholderOverhead := len(placeholder) - 1 // Subtract 1 for the '?' that gets replaced

	switch dt := dataType.(type) {
	case *arrow.Int8Type:
		// strconv.Itoa(int(x)) -> "-128" = 4 bytes
		dataSize = 4
	case *arrow.Int16Type:
		// strconv.Itoa(int(x)) -> "-32768" = 6 bytes
		dataSize = 6
	case *arrow.Int32Type:
		// strconv.Itoa(int(x)) -> "-2147483648" = 11 bytes
		dataSize = 11
	case *arrow.Int64Type:
		// strconv.FormatInt(x, 10) -> "-9223372036854775808" = 20 bytes
		dataSize = 20
	case *arrow.Uint8Type, *arrow.Uint16Type:
		// strconv.FormatUint() -> "65535" = 5 bytes
		dataSize = 5
	case *arrow.Uint32Type:
		// strconv.FormatUint() -> "4294967295" = 10 bytes
		dataSize = 10
	case *arrow.Uint64Type:
		// strconv.FormatUint() -> "18446744073709551615" = 20 bytes
		dataSize = 20

	case *arrow.Float32Type, *arrow.Float64Type:
		// Trino uses Numeric type (string representation of number)
		// Worst case: max precision decimal or scientific notation
		dataSize = 25

	case *arrow.BooleanType:
		// strconv.FormatBool() -> "false" = 5 bytes
		dataSize = 5

	case *arrow.Date32Type, *arrow.Date64Type:
		// fmt.Sprintf("DATE '%04d-%02d-%02d'", ...) -> "DATE 'YYYY-MM-DD'" = 17 bytes
		dataSize = 17

	case *arrow.Time32Type:
		// fmt.Sprintf("TIME '%02d:%02d:%02d.%09d'", ...)
		// "TIME 'HH:MM:SS.nnnnnnnnn'" = 28 bytes
		dataSize = 28

	case *arrow.Time64Type:
		// Same as Time32, always uses nanoseconds: 28 bytes
		dataSize = 28

	case *arrow.TimestampType:
		// "TIMESTAMP " + time.Format("'2006-01-02 15:04:05.999999999'")
		// Without timezone: "TIMESTAMP 'YYYY-MM-DD HH:MM:SS.nnnnnnnnn'" = 41 bytes
		// With timezone: "TIMESTAMP 'YYYY-MM-DD HH:MM:SS.nnnnnnnnn Z07:00'" = 47 bytes
		if dt.TimeZone == "" {
			dataSize = 41
		} else {
			dataSize = 47
		}

	case *arrow.Decimal32Type, *arrow.Decimal64Type, *arrow.Decimal128Type, *arrow.Decimal256Type:
		// Serialized as string number: "-999...999.99"
		// Worst case: all digits + sign + decimal point
		precision := 38 // Default max
		if dec, ok := dt.(interface{ Precision() int32 }); ok {
			precision = int(dec.Precision())
		}
		dataSize = precision + 2 // sign + decimal point

	case *arrow.StringType, *arrow.LargeStringType:
		// "'" + strings.Replace(x, "'", "''", -1) + "'"
		// Worst case: every character is a single quote, so doubled + 2 for wrapping quotes
		if lengthStr, ok := field.Metadata.GetValue("sql.length"); ok {
			var length int
			if _, err := fmt.Sscanf(lengthStr, "%d", &length); err == nil {
				dataSize = length*2 + 2 // Worst: all quotes doubled, plus wrapping quotes
			} else {
				dataSize = 512 // Conservative: 255 chars all doubled + wrapping (255*2 + 2)
			}
		} else {
			dataSize = 512 // Conservative default: 255 chars
		}

	case *arrow.BinaryType, *arrow.LargeBinaryType:
		// "X'" + hex.EncodeToString(x) + "'" -> hex encoding doubles size + 3 bytes overhead
		if lengthStr, ok := field.Metadata.GetValue("sql.length"); ok {
			var length int
			if _, err := fmt.Sscanf(lengthStr, "%d", &length); err == nil {
				dataSize = length*2 + 3 // X'....'
			} else {
				dataSize = 513 // Conservative: 255 bytes hex-encoded + X'' wrapper (255*2 + 3)
			}
		} else {
			dataSize = 513 // Conservative default: 255 bytes
		}

	case *arrow.FixedSizeBinaryType:
		// "X'" + hex + "'" = size*2 + 3
		dataSize = dt.ByteWidth*2 + 3

	case *arrow.DurationType:
		// INTERVAL 'value' HOUR/MINUTE/SECOND
		// Worst case: "INTERVAL '-2147483648.999' SECOND" = 34 bytes
		dataSize = 34

	default:
		// Check for UUID extension
		if extName, ok := field.Metadata.GetValue("ARROW:extension:name"); ok && extName == "arrow.uuid" {
			// UUID serialized as string with quotes: "'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'" = 38 bytes
			dataSize = 38
		} else {
			// Unknown type, use conservative estimate
			dataSize = 100
		}
	}

	// Add placeholder overhead (e.g., CAST(value AS REAL) has 15 extra chars beyond just value)
	return dataSize + placeholderOverhead
}

// getParameterPlaceholder returns the appropriate SQL placeholder for Arrow types that need casting
func (c *trinoConnectionImpl) getParameterPlaceholder(field arrow.Field) string {
	if extName, exists := field.Metadata.GetValue("ARROW:extension:name"); exists && extName == "arrow.uuid" {
		return "CAST(? AS UUID)"
	}

	switch field.Type.(type) {
	case *arrow.Float32Type:
		return "CAST(? AS REAL)"
	case *arrow.Float64Type:
		return "CAST(? AS DOUBLE)"
	default:
		return "?"
	}
}

// createTableIfNeeded creates the table based on the ingest mode
func (c *trinoConnectionImpl) createTableIfNeeded(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string, schema *arrow.Schema, options *driverbase.BulkIngestOptions) error {
	switch options.Mode {
	case adbc.OptionValueIngestModeCreate:
		// Create the table (fail if exists)
		return c.createTable(ctx, conn, tableName, schema, false)
	case adbc.OptionValueIngestModeCreateAppend:
		// Create the table if it doesn't exist
		return c.createTable(ctx, conn, tableName, schema, true)
	case adbc.OptionValueIngestModeReplace:
		// Drop and recreate the table
		if err := c.dropTable(ctx, conn, tableName); err != nil {
			return err
		}
		return c.createTable(ctx, conn, tableName, schema, false)
	case adbc.OptionValueIngestModeAppend:
		// Table should already exist, do nothing
		return nil
	default:
		return c.ErrorHelper.InvalidArgument("unsupported ingest mode: %s", options.Mode)
	}
}

// createTable creates a Trino table from Arrow schema
func (c *trinoConnectionImpl) createTable(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string, schema *arrow.Schema, ifNotExists bool) error {
	var queryBuilder strings.Builder
	queryBuilder.WriteString("CREATE TABLE ")
	if ifNotExists {
		queryBuilder.WriteString("IF NOT EXISTS ")
	}
	queryBuilder.WriteString(quoteIdentifier(tableName))
	queryBuilder.WriteString(" (")

	for i, field := range schema.Fields() {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}

		queryBuilder.WriteString(quoteIdentifier(field.Name))
		queryBuilder.WriteString(" ")

		// Convert Arrow type to Trino type
		trinoType := c.arrowToTrinoType(field)
		queryBuilder.WriteString(trinoType)
	}

	queryBuilder.WriteString(")")

	_, err := conn.ExecContext(ctx, queryBuilder.String())
	return err
}

// dropTable drops a Trino table
func (c *trinoConnectionImpl) dropTable(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string) error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(tableName))
	_, err := conn.ExecContext(ctx, dropSQL)
	return err
}

// arrowToTrinoType converts Arrow data type to Trino column type
func (c *trinoConnectionImpl) arrowToTrinoType(field arrow.Field) string {
	if extName, exists := field.Metadata.GetValue("ARROW:extension:name"); exists && extName == "arrow.uuid" {
		return "UUID"
	}

	var trinoType string

	switch arrowType := field.Type.(type) {
	case *arrow.BooleanType:
		trinoType = "BOOLEAN"
	case *arrow.Int8Type:
		trinoType = "TINYINT"
	case *arrow.Int16Type:
		trinoType = "SMALLINT"
	case *arrow.Int32Type:
		trinoType = "INTEGER"
	case *arrow.Int64Type:
		trinoType = "BIGINT"
	case *arrow.Float32Type:
		trinoType = "REAL"
	case *arrow.Float64Type:
		trinoType = "DOUBLE"
	case *arrow.StringType:
		trinoType = "VARCHAR"
	case *arrow.BinaryType:
		trinoType = "VARBINARY"
	case *arrow.BinaryViewType:
		trinoType = "VARBINARY"
	case *arrow.FixedSizeBinaryType:
		trinoType = "VARBINARY"
	case *arrow.LargeBinaryType:
		trinoType = "VARBINARY"
	case *arrow.Date32Type:
		trinoType = "DATE"
	case *arrow.TimestampType:

		// Determine precision based on Arrow timestamp unit
		var precision string
		switch arrowType.Unit {
		case arrow.Second:
			precision = "(0)"
		case arrow.Millisecond:
			precision = "(3)"
		case arrow.Microsecond:
			precision = "(6)"
		case arrow.Nanosecond:
			precision = "(9)"
		default:
			// should never happen, but panic here for defensive programming
			panic(fmt.Sprintf("unexpected Arrow timestamp unit: %v", arrowType.Unit))
		}

		// Use TIMESTAMP for timezone-naive timestamps, TIMESTAMP WITH TIME ZONE for timezone-aware
		if arrowType.TimeZone != "" {
			// Timezone-aware (timestamptz) -> TIMESTAMP WITH TIME ZONE
			trinoType = "TIMESTAMP" + precision + " WITH TIME ZONE"
		} else {
			// Timezone-naive (timestamp) -> TIMESTAMP
			trinoType = "TIMESTAMP" + precision
		}

	case *arrow.Time32Type:
		// Determine precision based on Arrow time unit
		switch arrowType.Unit {
		case arrow.Second:
			trinoType = "TIME(0)"
		case arrow.Millisecond:
			trinoType = "TIME(3)"
		default:
			// should never happen, but panic here for defensive programming
			panic(fmt.Sprintf("unexpected Time32 unit: %v", arrowType.Unit))
		}

	case *arrow.Time64Type:
		// Determine precision based on Arrow time unit
		switch arrowType.Unit {
		case arrow.Microsecond:
			trinoType = "TIME(6)"
		case arrow.Nanosecond:
			trinoType = "TIME(9)"
		default:
			// should never happen, but panic here for defensive programming
			panic(fmt.Sprintf("unexpected Time64 unit: %v", arrowType.Unit))
		}
	case arrow.DecimalType:
		trinoType = fmt.Sprintf("DECIMAL(%d,%d)", arrowType.GetPrecision(), arrowType.GetScale())
	default:
		// Default to VARCHAR for unknown types
		trinoType = "VARCHAR"
	}

	// Note: In Trino, columns are nullable by default, and assume that all columns are nullable since trino go client does not provide clean way to get nullability.
	return trinoType
}

// ListTableTypes implements driverbase.TableTypeLister interface
func (c *trinoConnectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	// Trino supports these standard table types
	return []string{
		"BASE TABLE", // Regular tables
		"VIEW",       // Views
	}, nil
}

// quoteIdentifier properly quotes a SQL identifier, escaping any internal quotes
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
