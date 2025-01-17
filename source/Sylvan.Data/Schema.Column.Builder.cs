using System;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data;

partial class Schema
{
	/// <summary>
	/// A column in the schema.
	/// </summary>
	partial class Column
	{
		/// <summary>
		/// A builder for a <see cref="Schema.Column"/>.
		/// </summary>
		public sealed class Builder
		{
			Column? column;

			Column Column => column ?? throw new InvalidOperationException();

			/// <summary>
			/// Creates a new column builder.
			/// </summary>
			public Builder()
			{
				column = new Column();
			}

			/// <summary>
			/// Creates a new column builder.
			/// </summary>
			public Builder(string name, Type type, bool allowNull = true)
				: this()
			{
				this.SetType(type);
				this.ColumnName = name;
				this.AllowDBNull = allowNull;
			}

			/// <summary>
			/// Creates a new column builder.
			/// </summary>
			public Builder(string name, DbType commonType, bool allowNull = true)
				: this()
			{
				var type = DataBinder.GetDataType(commonType);
				this.SetType(type);
				this.CommonDataType = commonType;
				this.ColumnName = name;
				this.AllowDBNull = allowNull;
			}

			/// <summary>
			/// Sets the column type.
			/// </summary>
			public void SetType(Type type)
			{
				this.DataType = type;
				this.DataTypeName = type.Name;
				this.NumericScale = 0;
				this.CommonDataType = DataBinder.GetDbType(type);

				switch (Type.GetTypeCode(type))
				{
					case TypeCode.Boolean:
						this.NumericPrecision = 1;
						this.ColumnSize = 1;
						break;
					case TypeCode.Char:
						this.NumericPrecision = 5;
						this.ColumnSize = 2;
						break;
					case TypeCode.Byte:
						this.NumericPrecision = 3;
						this.ColumnSize = 1;
						break;
					case TypeCode.Int16:
						this.NumericPrecision = 5;
						this.ColumnSize = 2;
						break;
					case TypeCode.Int32:
						this.NumericPrecision = 10;
						this.ColumnSize = 4;
						break;
					case TypeCode.Int64:
						this.NumericPrecision = 19;
						this.ColumnSize = 8;
						break;
					case TypeCode.Single:
						this.NumericPrecision = 7;
						this.ColumnSize = 4;
						break;
					case TypeCode.Double:
						this.NumericPrecision = 7;
						this.ColumnSize = 8;
						break;
					case TypeCode.Decimal:
						this.NumericPrecision = 28;
						this.ColumnSize = 16;
						break;
					case TypeCode.DateTime:
						this.NumericPrecision = 27;
						this.NumericScale = 7;
						this.ColumnSize = 8;
						break;
					case TypeCode.String:
						break;
					case TypeCode.Object:
						if (type == typeof(byte[]))
						{
							break;
						}
						if (type == typeof(Guid))
						{
							this.ColumnSize = 16;
							break;
						}
						if (type == typeof(DateTimeOffset))
						{
							this.NumericPrecision = 27;
							this.NumericScale = 7;
							this.ColumnSize = 16;
							break;
						}
#if NET6_0_OR_GREATER
						// TODO: populate other metadata for these types?
						if (type == typeof(DateOnly))
						{
							break;
						}

						if (type == typeof(TimeOnly))
						{
							break;
						}
#endif
						break;
				}
			}

			/// <summary>
			/// Creates a new column builder, copying an existing DbColumn.
			/// </summary>
			public Builder(DbColumn col)
			{
				var dataType = col.DataType ?? typeof(object);
				var underlyingType = Nullable.GetUnderlyingType(dataType);
				dataType = underlyingType ?? dataType;

				column = new Column
				{
					AllowDBNull = underlyingType != null || col.AllowDBNull == true,
					BaseCatalogName = col.BaseCatalogName,
					BaseColumnName = col.BaseColumnName,
					BaseSchemaName = col.BaseSchemaName,
					BaseServerName = col.BaseServerName,
					BaseTableName = col.BaseTableName,
					ColumnName = col.ColumnName,
					ColumnOrdinal = col.ColumnOrdinal,
					ColumnSize = col.ColumnSize,
					DataType = dataType,
					DataTypeName = col.DataTypeName,
					IsAliased = col.IsAliased,
					IsAutoIncrement = col.IsAutoIncrement,
					IsExpression = col.IsExpression,
					IsHidden = col.IsHidden,
					IsIdentity = col.IsIdentity,
					IsKey = col.IsKey,
					IsLong = col.IsLong,
					IsReadOnly = col.IsReadOnly,
					IsUnique = col.IsUnique,
					NumericPrecision = col.NumericPrecision,
					NumericScale = col.NumericScale,
					UdtAssemblyQualifiedName = col.UdtAssemblyQualifiedName
				};

				if (col is Column sc)
				{
					column.CommonDataType = sc.CommonDataType;
					column.SeriesOrdinal = sc.SeriesOrdinal;
					column.SeriesName = sc.SeriesName;
					column.SeriesHeaderFormat = sc.SeriesHeaderFormat;
					column.IsSeries = sc.IsSeries;
					column.SeriesType = sc.SeriesType;
					column.Format = sc.Format;
					column.BaseColumnOrdinal = sc.BaseColumnOrdinal;
				}
			}

			#region base properties

			/// <summary>
			/// Gets or sets a value indicating if the column allows nulls.
			/// </summary>
			public bool? AllowDBNull
			{
				get => Column.AllowDBNull;
				set => Column.AllowDBNull = value;
			}

			/// <summary>
			/// Gets or sets the base catalog name.
			/// </summary>
			public string? BaseCatalogName
			{
				get => Column.BaseCatalogName;
				set => Column.BaseCatalogName = value;
			}

			/// <summary>
			/// Gets or sets the base column name.
			/// </summary>
			public string? BaseColumnName
			{
				get => Column.BaseColumnName;
				set => Column.BaseColumnName = value;
			}

			/// <summary>
			/// Gets or sets the base schema name.
			/// </summary>
			public string? BaseSchemaName
			{
				get => Column.BaseSchemaName;
				set => Column.BaseSchemaName = value;
			}

			/// <summary>
			/// Gets or sets the base server name.
			/// </summary>
			public string? BaseServerName
			{
				get => Column.BaseServerName;
				set => Column.BaseServerName = value;
			}

			/// <summary>
			/// Gets or sets the base table name.
			/// </summary>
			public string? BaseTableName
			{
				get => Column.BaseTableName;
				set => Column.BaseTableName = value;
			}

			/// <summary>
			/// Gets or sets the column ordinal.
			/// </summary>
			public int? ColumnOrdinal
			{
				get => Column.ColumnOrdinal;
				set => Column.ColumnOrdinal = value;
			}

			/// <summary>
			/// Gets or sets the column name.
			/// </summary>
			public string ColumnName
			{
				get => Column.ColumnName;
				set => Column.ColumnName = value;
			}

			/// <summary>
			/// Gets or sets the column size.
			/// </summary>
			public int? ColumnSize
			{
				get => Column.ColumnSize;
				set => Column.ColumnSize = value;
			}

			/// <summary>
			/// Gets or sets the data type.
			/// </summary>
			public Type? DataType
			{
				get => Column.DataType;
				set => Column.DataType = value;
			}

			/// <summary>
			/// Gets or sets the data type name.
			/// </summary>
			public string? DataTypeName
			{
				get => Column.DataTypeName;
				set => Column.DataTypeName = value;
			}

			/// <summary>
			/// Gets or sets a value indicating if the column is aliased.
			/// </summary>
			public bool? IsAliased
			{
				get => Column.IsAliased;
				set => Column.IsAliased = value;
			}

			/// <summary>
			/// Gets or sets a value indicating if the column is auto-increment.
			/// </summary>
			public bool? IsAutoIncrement
			{
				get => Column.IsAutoIncrement;
				set => Column.IsAutoIncrement = value;
			}

			/// <summary>
			/// Gets or sets a value indicating if the column is an expression.
			/// </summary>
			public bool? IsExpression
			{
				get => Column.IsExpression;
				set => Column.IsExpression = value;
			}

			/// <summary>
			/// Gets or sets a value indicating if the column is hidden.
			/// </summary>
			public bool? IsHidden
			{
				get => Column.IsHidden;
				set => Column.IsHidden = value;
			}

			/// <summary>
			/// Gets or sets a value indicating if the column is an identity column.
			/// </summary>
			public bool? IsIdentity
			{
				get => Column.IsIdentity;
				set => Column.IsIdentity = value;
			}

			/// <summary>
			/// Gets or sets a value indicating if the column is a key column.
			/// </summary>
			public bool? IsKey
			{
				get => Column.IsKey;
				set => Column.IsKey = value;
			}

			/// <summary>
			/// Gets or sets a value indicating if the column is long.
			/// </summary>
			public bool? IsLong
			{
				get => Column.IsLong;
				set => Column.IsLong = value;
			}

			/// <summary>
			/// Gets or sets a value indicating if the column is read-only.
			/// </summary>
			public bool? IsReadOnly
			{
				get => Column.IsReadOnly;
				set => Column.IsReadOnly = value;
			}

			/// <summary>
			/// Gets or sets a value indicating if the column is unique.
			/// </summary>
			public bool? IsUnique
			{
				get => Column.IsUnique;
				set => Column.IsUnique = value;
			}

			/// <summary>
			/// Gets or sets the numeric precision of the column.
			/// </summary>
			public int? NumericPrecision
			{
				get => Column.NumericPrecision;
				set => Column.NumericPrecision = value;
			}

			/// <summary>
			/// Gets or sets the scale precision of the column.
			/// </summary>
			public int? NumericScale
			{
				get => Column.NumericScale;
				set => Column.NumericScale = value;
			}

			/// <summary>
			/// Gets or sets user-defined assembly qualified name.
			/// </summary>
			public string? UdtAssemblyQualifiedName
			{
				get => Column.UdtAssemblyQualifiedName;
				set => Column.UdtAssemblyQualifiedName = value;
			}

			#endregion

			#region custom properties

			/// <summary>
			/// Gets or sets the base column ordinal.
			/// </summary>
			public int? BaseColumnOrdinal
			{
				get => Column.BaseColumnOrdinal;
				set => Column.BaseColumnOrdinal = value;
			}

			/// <summary>
			/// Gets or sets the column data type.
			/// </summary>
			public DbType? CommonDataType
			{
				get => Column.CommonDataType;
				set => Column.CommonDataType = value;
			}

			internal string? Format
			{
				get => Column.Format;
				set => Column.Format = value;
			}

			internal bool? IsSeries
			{
				get => Column.IsSeries;
				set => Column.IsSeries = value;
			}

			internal Type? SeriesType
			{
				get => Column.SeriesType;
				set => Column.SeriesType = value;
			}

			internal string? SeriesName
			{
				get => Column.SeriesName;
				set => Column.SeriesName = value;
			}

			internal int? SeriesOrdinal
			{
				get => Column.SeriesOrdinal;
				set => Column.SeriesOrdinal = value;
			}

			internal string? SeriesHeaderFormat
			{
				get => Column.SeriesHeaderFormat;
				set => Column.SeriesHeaderFormat = value;
			}

			#endregion

			/// <summary>
			/// Builds the immutable Schema.Column.
			/// </summary>
			public Column Build()
			{
				var col = Column;
				column = null;
				return col;
			}
		}
	}
}