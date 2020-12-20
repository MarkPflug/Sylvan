using System;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data
{
	partial class Schema
	{
		public partial class Column : DbColumn
		{
			const string SeriesSymbol = "*";
			
			public int? BaseColumnOrdinal { get; protected set; }

			public DbType? CommonDataType { get; private set; }

			internal int? SeriesOrdinal { get; private set; }

			internal string? SeriesName { get; private set; }

			internal string? SeriesHeaderFormat { get; private set; }

			internal bool? IsSeries { get; private set; }

			internal Type? SeriesType { get; private set; }

			internal string? Format { get; private set; }


			public override object? this[string property]
			{
				get
				{
					switch (property)
					{
						case nameof(CommonDataType):
							return this.CommonDataType;
						case nameof(SeriesOrdinal):
							return this.SeriesOrdinal;
						case nameof(SeriesName):
							return this.SeriesName;
						case nameof(SeriesHeaderFormat):
							return this.SeriesHeaderFormat;
						case nameof(IsSeries):
							return this.IsSeries;
						case nameof(SeriesType):
							return this.SeriesType;
						case nameof(Format):
							return this.Format;
						case nameof(BaseColumnOrdinal):
							return this.BaseColumnOrdinal;
						default:
							return base[property];
					}
				}
			}

//			public Column(int? ordinal, string? baseName, string name, DbType type, bool allowNull, int? size = null, string? format = null)
//			{
//				this.ColumnOrdinal = ordinal;
//				this.CommonDataType = type;

//#warning this feels like the wrong place to deal with series.

//				var isSeries = name.EndsWith(SeriesSymbol) == true;
//				if (isSeries == false)
//				{
//					this.BaseColumnName = baseName;
//					this.ColumnName = name;
//				}
//				else
//				{
//					this.IsSeries = true;
//					this.SeriesOrdinal = 0;
//					this.SeriesName = name.Substring(0, name.Length - 1);
//					this.SeriesHeaderFormat = baseName;
//					if (SeriesHeaderFormat?.IndexOf(DateSeriesMarker, StringComparison.OrdinalIgnoreCase) >= 0)
//					{
//						this.SeriesType = typeof(DateTime);
//					}
//					if (SeriesHeaderFormat?.IndexOf(IntegerSeriesMarker, StringComparison.OrdinalIgnoreCase) >= 0)
//					{
//						this.SeriesType = typeof(int);
//					}
//				}

//				this.AllowDBNull = allowNull;
//				this.ColumnSize = size;
//				this.DataType = GetDataType(type);
//				this.DataTypeName = type.ToString();
//				this.Format = format;
//			}

//			public Column(DbColumn col)
//			{
//				this.CommonDataType = GetDbType(col.DataType);
//				this.ColumnName = col.ColumnName;
//				this.ColumnOrdinal = col.ColumnOrdinal;
//				this.BaseColumnName = col.BaseColumnName;
//				this.DataType = col.DataType;
//				this.ColumnSize = col.ColumnSize;
//				this.AllowDBNull = col.AllowDBNull;
//				this.IsUnique = col.IsUnique;
//				this.IsLong = col.IsLong;
//				this.NumericPrecision = col.NumericPrecision;
//				this.NumericScale = col.NumericScale;
//				this.Format = col[nameof(Format)] as string;
//			}

			private Column() { }
		}
	}
}
