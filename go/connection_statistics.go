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
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type trinoTableRef struct {
	catalog string
	schema  string
	table   string
}

type trinoStatistic struct {
	tableName  string
	columnName *string
	key        int16
	valueKind  arrow.UnionTypeCode
	valueI64   int64
	valueU64   uint64
	valueF64   float64
	valueBin   []byte
	approx     bool
}

// GetStatistics returns table and column statistics.
//
// Trino statistics are always approximate (marked statistic_is_approximate=true).
// The 'approximate' parameter controls error handling:
//   - true:  skip tables that error (best-effort)
//   - false: fail on any error (strict)
func (c *trinoConnectionImpl) GetStatistics(ctx context.Context, catalog, dbSchema, tableName *string, approximate bool) (array.RecordReader, error) {
	// ADBC semantics: empty string means "only objects without this property".
	// Trino always has catalog/schema/table names, so these filters produce no results.
	if (catalog != nil && *catalog == "") || (dbSchema != nil && *dbSchema == "") || (tableName != nil && *tableName == "") {
		return c.emptyGetStatisticsReader()
	}

	tables, err := c.getStatisticsTables(ctx, catalog, dbSchema, tableName)
	if err != nil {
		return nil, err
	}

	statsByCatalog := map[string]map[string][]trinoStatistic{}
	var catalogOrder []string
	schemaOrder := map[string][]string{}
	seenCatalog := map[string]bool{}
	seenSchema := map[string]map[string]bool{}

	for _, tbl := range tables {
		addCatalogSchemaToMaps(tbl.catalog, tbl.schema, seenCatalog, &catalogOrder, statsByCatalog, schemaOrder, seenSchema)

		stats, err := c.getTableStatistics(ctx, tbl, approximate)
		if err != nil {
			// When approximate is requested, be best-effort and skip tables that
			// error (connector/table may not support statistics).
			if approximate {
				continue
			}
			return nil, err
		}

		if len(stats) == 0 {
			continue
		}
		statsByCatalog[tbl.catalog][tbl.schema] = append(statsByCatalog[tbl.catalog][tbl.schema], stats...)
	}

	return c.buildGetStatisticsReader(catalogOrder, schemaOrder, statsByCatalog)
}

// addCatalogSchemaToMaps tracks catalog and schema in the statistics collection maps,
// maintaining insertion order and initializing nested structures as needed.
func addCatalogSchemaToMaps(
	cat, sch string,
	seenCatalog map[string]bool,
	catalogOrder *[]string,
	statsByCatalog map[string]map[string][]trinoStatistic,
	schemaOrder map[string][]string,
	seenSchema map[string]map[string]bool,
) {
	if !seenCatalog[cat] {
		seenCatalog[cat] = true
		*catalogOrder = append(*catalogOrder, cat)
		statsByCatalog[cat] = map[string][]trinoStatistic{}
		schemaOrder[cat] = nil
	}
	if seenSchema[cat] == nil {
		seenSchema[cat] = map[string]bool{}
	}
	if !seenSchema[cat][sch] {
		seenSchema[cat][sch] = true
		schemaOrder[cat] = append(schemaOrder[cat], sch)
	}
}

func (c *trinoConnectionImpl) GetStatisticNames(ctx context.Context) (array.RecordReader, error) {
	bldr := array.NewRecordBuilder(c.Alloc, adbc.GetStatisticNamesSchema)
	defer bldr.Release()

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return array.NewRecordReader(adbc.GetStatisticNamesSchema, []arrow.RecordBatch{rec})
}

func (c *trinoConnectionImpl) emptyGetStatisticsReader() (array.RecordReader, error) {
	bldr := array.NewRecordBuilder(c.Alloc, adbc.GetStatisticsSchema)
	defer bldr.Release()

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return array.NewRecordReader(adbc.GetStatisticsSchema, []arrow.RecordBatch{rec})
}

func (c *trinoConnectionImpl) getStatisticsTables(ctx context.Context, catalog, dbSchema, tableName *string) (tables []trinoTableRef, err error) {
	var queryBuilder strings.Builder

	// Use the appropriate metadata source based on catalog filter:
	// - If catalog is a literal (no wildcards), qualify information_schema with it
	// - Otherwise, use system.jdbc.tables which provides cross-catalog access
	useCatalogQualified := catalog != nil && !strings.ContainsAny(*catalog, "%_")

	if useCatalogQualified {
		// Query specific catalog's information_schema
		fmt.Fprintf(&queryBuilder, `
			SELECT table_catalog, table_schema, table_name
			FROM %s.information_schema.tables
			WHERE table_type = 'BASE TABLE'`, quoteIdentifier(*catalog))
	} else {
		// Query system catalog for cross-catalog access
		queryBuilder.WriteString(`
			SELECT table_cat, table_schem, table_name
			FROM system.jdbc.tables
			WHERE table_type = 'TABLE'`)
	}

	args := []any{}
	if catalog != nil {
		if useCatalogQualified {
			// Already qualified, no filter needed
		} else {
			// Apply LIKE filter for patterns or when catalog is nil
			queryBuilder.WriteString(` AND table_cat LIKE ?`)
			args = append(args, *catalog)
		}
	}
	if dbSchema != nil {
		if useCatalogQualified {
			queryBuilder.WriteString(` AND table_schema LIKE ?`)
		} else {
			queryBuilder.WriteString(` AND table_schem LIKE ?`)
		}
		args = append(args, *dbSchema)
	}
	if tableName != nil {
		queryBuilder.WriteString(` AND table_name LIKE ?`)
		args = append(args, *tableName)
	}

	if useCatalogQualified {
		queryBuilder.WriteString(` ORDER BY table_catalog, table_schema, table_name`)
	} else {
		queryBuilder.WriteString(` ORDER BY table_cat, table_schem, table_name`)
	}

	rows, err := c.Conn.QueryContext(ctx, queryBuilder.String(), args...)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to query tables for statistics")
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	for rows.Next() {
		var cat, sch, tbl string
		if err := rows.Scan(&cat, &sch, &tbl); err != nil {
			return nil, c.ErrorHelper.WrapIO(err, "failed to scan table for statistics")
		}
		tables = append(tables, trinoTableRef{catalog: cat, schema: sch, table: tbl})
	}
	if err := rows.Err(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "error during table iteration for statistics")
	}

	return tables, nil
}

func (c *trinoConnectionImpl) getTableStatistics(ctx context.Context, tbl trinoTableRef, approximate bool) (stats []trinoStatistic, err error) {
	qualified := fmt.Sprintf("%s.%s.%s", quoteIdentifier(tbl.catalog), quoteIdentifier(tbl.schema), quoteIdentifier(tbl.table))
	query := "SHOW STATS FOR " + qualified

	rows, err := c.Conn.QueryContext(ctx, query)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to query stats for %s.%s.%s", tbl.catalog, tbl.schema, tbl.table)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	var tableRowCount float64
	var hasTableRowCount bool

	type colRow struct {
		columnName sql.NullString
		dataSize   any
		ndv        any
		nullFrac   any
		rowCount   any
		low        any
		high       any
	}
	var colRows []colRow

	for rows.Next() {
		var r colRow
		if err := rows.Scan(&r.columnName, &r.dataSize, &r.ndv, &r.nullFrac, &r.rowCount, &r.low, &r.high); err != nil {
			return nil, c.ErrorHelper.WrapIO(err, "failed to scan stats row for %s.%s.%s", tbl.catalog, tbl.schema, tbl.table)
		}

		if !r.columnName.Valid {
			if v, ok, convErr := coerceFloat64(r.rowCount); convErr != nil {
				return nil, c.ErrorHelper.WrapInternal(convErr, "failed to parse row_count for %s.%s.%s", tbl.catalog, tbl.schema, tbl.table)
			} else if ok {
				tableRowCount = v
				hasTableRowCount = true
			}
		} else {
			colRows = append(colRows, r)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "error during stats iteration for %s.%s.%s", tbl.catalog, tbl.schema, tbl.table)
	}

	// Trino statistics are always approximate (sampling-based, HyperLogLog, etc.).
	// Always mark statistic_is_approximate=true regardless of the 'approximate' parameter.
	isApprox := true
	_ = approximate // Ignored for marking statistics; only used for error handling (see GetStatistics)

	if hasTableRowCount {
		stats = append(stats, trinoStatistic{
			tableName: tbl.table,
			key:       int16(adbc.StatisticRowCountKey),
			valueKind: 2, // float64
			valueF64:  tableRowCount,
			approx:    isApprox,
		})
	}

	for _, r := range colRows {
		colName := r.columnName.String

		if v, ok, convErr := coerceFloat64(r.ndv); convErr != nil {
			return nil, c.ErrorHelper.WrapInternal(convErr, "failed to parse distinct_values_count for %s.%s.%s.%s", tbl.catalog, tbl.schema, tbl.table, colName)
		} else if ok {
			stats = append(stats, trinoStatistic{
				tableName:  tbl.table,
				columnName: &colName,
				key:        int16(adbc.StatisticDistinctCountKey),
				valueKind:  2, // float64
				valueF64:   v,
				approx:     isApprox,
			})
		}

		if hasTableRowCount {
			if frac, ok, convErr := coerceFloat64(r.nullFrac); convErr != nil {
				return nil, c.ErrorHelper.WrapInternal(convErr, "failed to parse nulls_fraction for %s.%s.%s.%s", tbl.catalog, tbl.schema, tbl.table, colName)
			} else if ok && !math.IsNaN(frac) {
				nullCount := frac * tableRowCount
				stats = append(stats, trinoStatistic{
					tableName:  tbl.table,
					columnName: &colName,
					key:        int16(adbc.StatisticNullCountKey),
					valueKind:  2, // float64
					valueF64:   nullCount,
					approx:     isApprox,
				})
			}
		}

		if lowStr, ok := coerceString(r.low); ok {
			stats = append(stats, trinoStatistic{
				tableName:  tbl.table,
				columnName: &colName,
				key:        int16(adbc.StatisticMinValueKey),
				valueKind:  3, // binary
				valueBin:   []byte(lowStr),
				approx:     isApprox,
			})
		}

		if highStr, ok := coerceString(r.high); ok {
			stats = append(stats, trinoStatistic{
				tableName:  tbl.table,
				columnName: &colName,
				key:        int16(adbc.StatisticMaxValueKey),
				valueKind:  3, // binary
				valueBin:   []byte(highStr),
				approx:     isApprox,
			})
		}

		// Trino SHOW STATS data_size is an estimated total size for the column.
		// Map this to ADBC average byte width by dividing by the (estimated) row count.
		if hasTableRowCount && tableRowCount > 0 {
			if sz, ok, convErr := coerceFloat64(r.dataSize); convErr != nil {
				return nil, c.ErrorHelper.WrapInternal(convErr, "failed to parse data_size for %s.%s.%s.%s", tbl.catalog, tbl.schema, tbl.table, colName)
			} else if ok {
				stats = append(stats, trinoStatistic{
					tableName:  tbl.table,
					columnName: &colName,
					key:        int16(adbc.StatisticAverageByteWidthKey),
					valueKind:  2, // float64
					valueF64:   sz / tableRowCount,
					approx:     isApprox,
				})
			}
		}
	}

	return stats, nil
}

func (c *trinoConnectionImpl) buildGetStatisticsReader(
	catalogOrder []string,
	schemaOrder map[string][]string,
	statsByCatalog map[string]map[string][]trinoStatistic,
) (array.RecordReader, error) {
	bldr := array.NewRecordBuilder(c.Alloc, adbc.GetStatisticsSchema)
	defer bldr.Release()

	catalogNameBldr := bldr.Field(0).(*array.StringBuilder)
	catalogSchemasBldr := bldr.Field(1).(*array.ListBuilder)
	dbSchemaStructBldr := catalogSchemasBldr.ValueBuilder().(*array.StructBuilder)
	dbSchemaNameBldr := dbSchemaStructBldr.FieldBuilder(0).(*array.StringBuilder)
	dbSchemaStatsListBldr := dbSchemaStructBldr.FieldBuilder(1).(*array.ListBuilder)

	statsStructBldr := dbSchemaStatsListBldr.ValueBuilder().(*array.StructBuilder)
	tableNameBldr := statsStructBldr.FieldBuilder(0).(*array.StringBuilder)
	columnNameBldr := statsStructBldr.FieldBuilder(1).(*array.StringBuilder)
	statKeyBldr := statsStructBldr.FieldBuilder(2).(*array.Int16Builder)
	statValueBldr := statsStructBldr.FieldBuilder(3).(*array.DenseUnionBuilder)
	statApproxBldr := statsStructBldr.FieldBuilder(4).(*array.BooleanBuilder)

	statI64Bldr := statValueBldr.Child(0).(*array.Int64Builder)
	statU64Bldr := statValueBldr.Child(1).(*array.Uint64Builder)
	statF64Bldr := statValueBldr.Child(2).(*array.Float64Builder)
	statBinBldr := statValueBldr.Child(3).(*array.BinaryBuilder)

	for _, cat := range catalogOrder {
		catalogNameBldr.Append(cat)
		catalogSchemasBldr.Append(true)

		for _, sch := range schemaOrder[cat] {
			dbSchemaStructBldr.Append(true)
			dbSchemaNameBldr.Append(sch)
			dbSchemaStatsListBldr.Append(true)

			for _, st := range statsByCatalog[cat][sch] {
				statsStructBldr.Append(true)
				tableNameBldr.Append(st.tableName)
				if st.columnName == nil {
					columnNameBldr.AppendNull()
				} else {
					columnNameBldr.Append(*st.columnName)
				}
				statKeyBldr.Append(st.key)
				statApproxBldr.Append(st.approx)

				statValueBldr.Append(st.valueKind)
				switch st.valueKind {
				case 0:
					statI64Bldr.Append(st.valueI64)
				case 1:
					statU64Bldr.Append(st.valueU64)
				case 2:
					statF64Bldr.Append(st.valueF64)
				case 3:
					statBinBldr.Append(st.valueBin)
				default:
					return nil, c.ErrorHelper.Errorf(adbc.StatusInternal, "unknown statistic value kind: %d", st.valueKind)
				}
			}
		}
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return array.NewRecordReader(adbc.GetStatisticsSchema, []arrow.RecordBatch{rec})
}

func coerceString(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	switch s := v.(type) {
	case string:
		if s == "" {
			return "", true
		}
		return s, true
	case []byte:
		return string(s), true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

func coerceFloat64(v any) (float64, bool, error) {
	if v == nil {
		return 0, false, nil
	}

	switch n := v.(type) {
	case float64:
		return n, true, nil
	case float32:
		return float64(n), true, nil
	case int64:
		return float64(n), true, nil
	case int32:
		return float64(n), true, nil
	case int:
		return float64(n), true, nil
	case uint64:
		return float64(n), true, nil
	case uint32:
		return float64(n), true, nil
	case uint:
		return float64(n), true, nil
	case sql.NullFloat64:
		if !n.Valid {
			return 0, false, nil
		}
		return n.Float64, true, nil
	case sql.NullInt64:
		if !n.Valid {
			return 0, false, nil
		}
		return float64(n.Int64), true, nil
	case []byte:
		if len(n) == 0 {
			return 0, false, nil
		}
		f, err := strconv.ParseFloat(string(n), 64)
		if err != nil {
			return 0, false, err
		}
		return f, true, nil
	case string:
		if strings.TrimSpace(n) == "" {
			return 0, false, nil
		}
		f, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0, false, err
		}
		return f, true, nil
	default:
		// Some Trino types (e.g. DECIMAL) can scan as driver-specific types.
		s := fmt.Sprintf("%v", v)
		if strings.TrimSpace(s) == "" {
			return 0, false, nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, false, err
		}
		return f, true, nil
	}
}
