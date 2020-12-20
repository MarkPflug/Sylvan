using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data
{
	partial class Schema
	{
		partial class Column
		{
			public sealed class Builder
			{
				Column? column;

				Column Column => column ?? throw new InvalidOperationException();

				public Builder()
				{
					column = new Column();
				}

				public Builder(string name, Type type, bool allowNull = true)
					: this()
				{
					this.SetType(type);
					this.ColumnName = name;
					this.AllowDBNull = allowNull;
				}

				public Builder(string name, DbType commonType, bool allowNull = true)
					: this()
				{
					var type = GetDataType(commonType);
					this.SetType(type);
					this.CommonDataType = commonType;
					this.ColumnName = name;
					this.AllowDBNull = allowNull;
				}

				public void SetType(Type type)
				{
					this.DataType = type;
					this.DataTypeName = type.Name;
					this.NumericScale = 0;
					this.CommonDataType = GetDbType(type);

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
							if(type == typeof(byte[]))
							{
								break;
							}
							if (type == typeof(Guid))
							{
								this.ColumnSize = 16;
								break;
							}
							goto default;
						default:
							throw new NotSupportedException();
					}
				}

				public Builder(DbColumn col)
				{
					column = new Column();
					column.AllowDBNull = col.AllowDBNull;
					column.BaseCatalogName = col.BaseCatalogName;
					column.BaseColumnName = col.BaseColumnName;
					column.BaseSchemaName = col.BaseSchemaName;
					column.BaseServerName = col.BaseServerName;
					column.BaseTableName = col.BaseTableName;
					column.ColumnName = col.ColumnName;
					column.ColumnOrdinal = col.ColumnOrdinal;
					column.ColumnSize = col.ColumnSize;
					column.DataType = col.DataType;
					column.DataTypeName = col.DataTypeName;
					column.IsAliased = col.IsAliased;
					column.IsAutoIncrement = col.IsAutoIncrement;
					column.IsExpression = col.IsExpression;
					column.IsHidden = col.IsHidden;
					column.IsIdentity = col.IsIdentity;
					column.IsKey = col.IsKey;
					column.IsLong = col.IsLong;
					column.IsReadOnly = col.IsReadOnly;
					column.IsUnique = col.IsUnique;
					column.NumericPrecision = col.NumericPrecision;
					column.NumericScale = col.NumericScale;
					column.UdtAssemblyQualifiedName = col.UdtAssemblyQualifiedName;

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

				public bool? AllowDBNull
				{
					get => Column.AllowDBNull;
					set => Column.AllowDBNull = value;
				}

				public string? BaseCatalogName
				{
					get => Column.BaseCatalogName;
					set => Column.BaseCatalogName = value;
				}

				public string? BaseColumnName
				{
					get => Column.BaseColumnName;
					set => Column.BaseColumnName = value;
				}

				public string? BaseSchemaName
				{
					get => Column.BaseSchemaName;
					set => Column.BaseSchemaName = value;
				}

				public string? BaseServerName
				{
					get => Column.BaseServerName;
					set => Column.BaseServerName = value;
				}

				public string? BaseTableName
				{
					get => Column.BaseTableName;
					set => Column.BaseTableName = value;
				}

				public int? ColumnOrdinal
				{
					get => Column.ColumnOrdinal;
					set => Column.ColumnOrdinal = value;
				}

				public string ColumnName
				{
					get => Column.ColumnName;
					set => Column.ColumnName = value;
				}

				public int? ColumnSize
				{
					get => Column.ColumnSize;
					set => Column.ColumnSize = value;
				}

				public Type? DataType
				{
					get => Column.DataType;
					set => Column.DataType = value;
				}

				public string? DataTypeName
				{
					get => Column.DataTypeName;
					set => Column.DataTypeName = value;
				}


				public bool? IsAliased
				{
					get => Column.IsAliased;
					set => Column.IsAliased = value;
				}

				public bool? IsAutoIncrement
				{
					get => Column.IsAutoIncrement;
					set => Column.IsAutoIncrement = value;
				}

				public bool? IsExpression
				{
					get => Column.IsExpression;
					set => Column.IsExpression = value;
				}

				public bool? IsHidden
				{
					get => Column.IsHidden;
					set => Column.IsHidden = value;
				}

				public bool? IsIdentity
				{
					get => Column.IsIdentity;
					set => Column.IsIdentity = value;
				}

				public bool? IsKey
				{
					get => Column.IsKey;
					set => Column.IsKey = value;
				}

				public bool? IsLong
				{
					get => Column.IsLong;
					set => Column.IsLong = value;
				}

				public bool? IsReadOnly
				{
					get => Column.IsReadOnly;
					set => Column.IsReadOnly = value;
				}

				public bool? IsUnique
				{
					get => Column.IsUnique;
					set => Column.IsUnique = value;
				}

				public int? NumericPrecision
				{
					get => Column.NumericPrecision;
					set => Column.NumericPrecision = value;
				}

				public int? NumericScale
				{
					get => Column.NumericScale;
					set => Column.NumericScale = value;
				}

				public string? UdtAssemblyQualifiedName
				{
					get => Column.UdtAssemblyQualifiedName;
					set => Column.UdtAssemblyQualifiedName = value;
				}

				#endregion

				#region custom properties

				public int? BaseColumnOrdinal
				{
					get => Column.BaseColumnOrdinal;
					set => Column.BaseColumnOrdinal = value;
				}

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

				public Column Build()
				{
					var col = Column;
					column = null;
					return col;
				}


				internal static Type GetDataType(DbType type)
				{
					switch (type)
					{
						case DbType.Boolean:
							return typeof(bool);
						case DbType.Byte: 
							return typeof(byte);
						case DbType.Int16: 
							return typeof(short);
						case DbType.Int32: 
							return typeof(int);
						case DbType.Int64: 
							return typeof(long);
						case DbType.Single: 
							return typeof(float);
						case DbType.Double: 
							return typeof(double);
						case DbType.Decimal:
						case DbType.VarNumeric:
						case DbType.Currency:
							return typeof(decimal);
						case DbType.String:
						case DbType.StringFixedLength:
						case DbType.AnsiString:
						case DbType.AnsiStringFixedLength:
							return typeof(string);
						case DbType.Binary: 
							return typeof(byte[]);
						case DbType.Guid: 
							return typeof(Guid);
						case DbType.DateTime:
						case DbType.DateTime2:
						case DbType.Date: 
							return typeof(DateTime);
					}
					throw new NotSupportedException();
				}

				internal static DbType? GetDbType(Type? type)
				{
					switch (Type.GetTypeCode(type))
					{
						case TypeCode.Boolean:
							return DbType.Boolean;
						case TypeCode.Byte:
							return DbType.Byte;
						case TypeCode.Char:
							return DbType.StringFixedLength;
						case TypeCode.Int16:
						case TypeCode.SByte:
							return DbType.Int16;
						case TypeCode.Int32:
						case TypeCode.UInt16:
							return DbType.Int32;
						case TypeCode.Int64:
						case TypeCode.UInt32:
							return DbType.Int64;
						case TypeCode.Single:
							return DbType.Single;
						case TypeCode.Double:
							return DbType.Double;
						case TypeCode.Decimal:
							return DbType.Decimal;
						case TypeCode.String:
							//more?
							return DbType.String;
						case TypeCode.DateTime:
							//more?
							return DbType.DateTime;
					}

					if (type == typeof(byte[]))
					{
						return DbType.Binary;
					}

					if (type == typeof(Guid))
					{
						return DbType.Guid;
					}
					return null;
				}

				
			}
		}
	}
}