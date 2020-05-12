using System;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data
{

	// This provides an implementation of DbDataReader.GetSchemaTable using the IDbColumnSchemaGenerator implementation.
	static class SchemaTable
	{
		public static DataTable GetSchemaTable(ReadOnlyCollection<DbColumn> schema)
		{
			// what an absolute nightmare...

			var table = new DataTable("SchemaTable");
			var cols = table.Columns;
			var nameCol = new DataColumn(SchemaTableColumn.ColumnName, typeof(string));
			var ordinalCol = new DataColumn(SchemaTableColumn.ColumnOrdinal, typeof(int));
			var sizeCol = new DataColumn(SchemaTableColumn.ColumnSize, typeof(int));
			var precCol = new DataColumn(SchemaTableColumn.NumericPrecision, typeof(short));
			var scaleCol = new DataColumn(SchemaTableColumn.NumericScale, typeof(short));
			var typeCol = new DataColumn(SchemaTableColumn.DataType, typeof(Type));
			var allowNullCol = new DataColumn(SchemaTableColumn.AllowDBNull, typeof(bool));

			var baseNameCol = new DataColumn(SchemaTableColumn.BaseColumnName, typeof(string));
			var baseSchemaCol = new DataColumn(SchemaTableColumn.BaseSchemaName, typeof(string));
			var baseTableCol = new DataColumn(SchemaTableColumn.BaseTableName, typeof(string));

			var isAliasedCol = new DataColumn(SchemaTableColumn.IsAliased, typeof(bool));
			var isExpressionCol = new DataColumn(SchemaTableColumn.IsExpression, typeof(bool));
			var isKeyCol = new DataColumn(SchemaTableColumn.IsKey, typeof(bool));
			var isLongCol = new DataColumn(SchemaTableColumn.IsLong, typeof(bool));
			var isUniqueCol = new DataColumn(SchemaTableColumn.IsUnique, typeof(bool));

			var providerTypeCol = new DataColumn(SchemaTableColumn.ProviderType, typeof(int));
			var nvProviderTypeCol = new DataColumn(SchemaTableColumn.NonVersionedProviderType, typeof(int));

			cols.Add(nameCol);
			cols.Add(ordinalCol);
			cols.Add(sizeCol);
			cols.Add(precCol);
			cols.Add(scaleCol);
			cols.Add(typeCol);
			cols.Add(allowNullCol);
			cols.Add(baseNameCol);
			cols.Add(baseSchemaCol);
			cols.Add(baseTableCol);
			cols.Add(isAliasedCol);
			cols.Add(isExpressionCol);
			cols.Add(isKeyCol);
			cols.Add(isLongCol);
			cols.Add(isUniqueCol);
			cols.Add(providerTypeCol);
			cols.Add(nvProviderTypeCol);

			foreach (var col in schema)
			{
				var row = table.NewRow();
				row[nameCol] = col.ColumnName ?? (object)DBNull.Value;
				row[ordinalCol] = col.ColumnOrdinal ?? (object)DBNull.Value;
				row[sizeCol] = col.ColumnSize ?? (object) DBNull.Value;

				if (col.DataType == typeof(int))
				{
					row[precCol] = 10;
					row[scaleCol] = 0;
				}
				else
				if (col.DataType == typeof(double))
				{
					row[precCol] = 15;
					row[scaleCol] = 0;
				}
				else
				{
					row[precCol] = DBNull.Value;
					row[scaleCol] = DBNull.Value;
				}


				row[typeCol] = col.DataType;
				row[allowNullCol] = col.AllowDBNull ?? (object)DBNull.Value;
				row[baseNameCol] = col.BaseColumnName ?? (object)DBNull.Value;
				row[baseSchemaCol] = col.BaseSchemaName ?? (object)DBNull.Value;
				row[baseTableCol] = col.BaseTableName ?? (object)DBNull.Value;

				row[isAliasedCol] = col.IsAliased ?? (object)DBNull.Value;
				row[isExpressionCol] = col.IsExpression ?? (object)DBNull.Value;

				row[isKeyCol] = col.IsKey ?? (object)DBNull.Value;
				row[isLongCol] = col.IsLong ?? (object) DBNull.Value;
				row[isUniqueCol] = col.IsUnique ?? (object)DBNull.Value;

				var code = (int)Type.GetTypeCode(col.DataType);
				row[providerTypeCol] = code;
				row[nvProviderTypeCol] = code;

				table.Rows.Add(row);
			}
			return table;
		}
	}
}
