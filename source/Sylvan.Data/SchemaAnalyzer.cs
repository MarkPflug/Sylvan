using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Sylvan.Data
{
	public sealed class SchemaAnalyzerOptions
	{
		internal static readonly SchemaAnalyzerOptions Default = new SchemaAnalyzerOptions();

		public SchemaAnalyzerOptions()
		{
			this.AnalyzeRowCount = 10000;
		}

		public int AnalyzeRowCount { get; set; }
	}

	public class ColumnSize
	{
		class NoneColumnSizer : IColumnSize
		{
			public int GetSize(int maxLen)
			{
				return maxLen;
			}
		}

		class ProgrammerNumberColumnSizer : IColumnSize
		{
			public int GetSize(int maxLen)
			{
				return maxLen;
			}
		}

		class HumanNumberColumnSizer : IColumnSize
		{
			public int GetSize(int maxLen)
			{
				return maxLen;
			}
		}

		class FixedColumnSizer : IColumnSize
		{
			readonly int size;
			public FixedColumnSizer(int size)
			{
				this.size = size;
			}

			public int GetSize(int maxLen)
			{
				return size;
			}
		}

		public static readonly IColumnSize None = new NoneColumnSizer();
		public static readonly IColumnSize Programmer = new ProgrammerNumberColumnSizer();
		public static readonly IColumnSize Human = new HumanNumberColumnSizer();

		public static readonly IColumnSize Size256 = new FixedColumnSizer(0x100);
		public static readonly IColumnSize Size1024 = new FixedColumnSizer(0x800);
	}
	
	public interface IColumnSize
	{
		int GetSize(int maxLen);
	}

	public sealed class SchemaAnalyzer
	{
		int rowCount;

		public SchemaAnalyzer(SchemaAnalyzerOptions? options = null)
		{
			options ??= SchemaAnalyzerOptions.Default;
			this.rowCount = options.AnalyzeRowCount;
		}

		public class AnalysisResult : IEnumerable<ColumnInfo>, IDbColumnSchemaGenerator
		{
			readonly ColumnInfo[] columns;

			public AnalysisResult(ColumnInfo[] columns)
			{
				this.columns = columns;
			}

			public ReadOnlyCollection<DbColumn> GetColumnSchema()
			{
				var cols = this.columns.Select(c => c.CreateColumnSchema()).ToArray();
				return new ReadOnlyCollection<DbColumn>(cols);
			}

			public IEnumerator<ColumnInfo> GetEnumerator()
			{
				foreach (var col in columns)
					yield return col;
			}

			IEnumerator IEnumerable.GetEnumerator()
			{
				return this.GetEnumerator();
			}
		}

		public AnalysisResult Analyze(DbDataReader dataReader)
		{
			return AnalyzeAsync(dataReader).Result;
		}

		public async Task<AnalysisResult> AnalyzeAsync(DbDataReader dataReader) {

			var colInfos = new ColumnInfo[dataReader.FieldCount];
			for (int i = 0; i < colInfos.Length; i++)
			{
				colInfos[i] = new ColumnInfo(i, dataReader.GetName(i));
			}

			var sw = Stopwatch.StartNew();
			int c = 0;
			while (await dataReader.ReadAsync() && c++ < rowCount)
			{
				for (int i = 0; i < dataReader.FieldCount; i++)
				{
					colInfos[i].Analyze(dataReader, i);
				}
			}
			sw.Stop();

			return new AnalysisResult(colInfos);
		}

		class ColumnSchema : DbColumn
		{
			public override string ToString()
			{
				var size = this.DataType == typeof(string) ? "[" + this.ColumnSize + "]" : "";
				return $"{this.ColumnName} {this.DataTypeName}{(this.AllowDBNull == true ? "?" : "")}{size}";
			}

			ColumnSchema(int ordinal, string? name, bool isNullable, bool isUnique)
			{
				this.ColumnOrdinal = ordinal;
				this.ColumnName = name?.Length > 64 ? name.Substring(0, 64) : name;
				this.AllowDBNull = isNullable;
				this.IsUnique = IsUnique;
			}

			public static ColumnSchema CreateString(int ordinal, string? name, bool isNullable, bool isUnique, int length, bool isAscii)
			{
				return
					new ColumnSchema(ordinal, name, isNullable, isUnique)
					{
						ColumnSize = length,
						NumericPrecision = isAscii ? 8 : 16,
						DataType = typeof(string),
						DataTypeName = "string",
						IsLong = false,
					};
			}

			public static ColumnSchema CreateText(int ordinal, string? name, bool isNullable)
			{
				return
					new ColumnSchema(ordinal, name, isNullable, false)
					{
						ColumnSize = int.MaxValue,
						NumericPrecision = 16,
						DataType = typeof(string),
						DataTypeName = "string",
						IsLong = true,
					};
			}

			public static ColumnSchema CreateDate(int ordinal, string? name, bool isNullable, bool isUnique, int? precision)
			{
				return
					new ColumnSchema(ordinal, name, isNullable, isUnique)
					{
						NumericPrecision = precision,
						DataType = typeof(DateTime),
						DataTypeName = precision == null ? "date" : "datetime",
					};
			}

			public static ColumnSchema CreateInt(int ordinal, string? name, bool isNullable, bool isUnique, Type type)
			{

				var (p, s, n) =
					(type == typeof(long))
					? (19, 8, "long")
					: (type == typeof(int))
					? (18, 4, "int")
					: (type == typeof(short))
					? (5, 2, "short")
					: (3, 1, "byte");

				return
					new ColumnSchema(ordinal, name, isNullable, isUnique)
					{
						DataType = type,
						DataTypeName = n,
						NumericPrecision = p,
						ColumnSize = s,
					};
			}

			public static ColumnSchema CreateFloat(int ordinal, string? name, bool isNullable, Type type)
			{
				var (p, s, n) =
					(type == typeof(double))
					? (15, 8, "double")
					: (7, 4, "float");

				return
					new ColumnSchema(ordinal, name, isNullable, false)
					{
						DataType = type,
						DataTypeName = n,
						NumericPrecision = p,
						ColumnSize = s,
					};
			}
		}

		public class ColumnInfo
		{
			internal ColumnInfo(int ordinal, string? name)
			{
				this.ordinal = ordinal;
				this.name = name;
				isInt = isFloat = isDate = isDateTime = isGuid = true;
				isNullable = false;
				dateHasFractionalSeconds = false;
				isUnique = true;
				intMax = long.MinValue;
				intMin = long.MaxValue;
				floatMax = double.MinValue;
				floatMin = double.MaxValue;
				dateMin = DateTime.MaxValue;
				dateMax = DateTime.MinValue;
				stringLenMax = 0;
				stringLenMin = int.MaxValue;
				stringLenTotal = 0;
				isAscii = true;

				this.valueCount = new Dictionary<string, int>();
			}

			public int Ordinal => this.ordinal;
			public string? Name => this.name;

			bool isAscii;
			int ordinal;
			string? name;
			bool isInt, isFloat, isDate, isDateTime, isGuid;
			bool dateHasFractionalSeconds;
			bool isNullable;
			bool isUnique;

			int count;
			int nullCount;

			long intMin, intMax;
			double floatMin, floatMax;
			DateTime dateMin, dateMax;
			int stringLenMax;
			int stringLenMin;
			long stringLenTotal;

			Dictionary<string, int> valueCount;

			public void Analyze(DbDataReader dr, int ordinal)
			{
				count++;
				var isNull = dr.IsDBNull(ordinal);
				if (isNull)
				{
					nullCount++;
					this.isNullable = true;
					return;
				}

				var str = dr.GetString(ordinal);

				if (isFloat || isInt || isDate || isDateTime || isGuid)
				{
					if (string.IsNullOrEmpty(str))
					{
						isNullable = true;
					}
					else
					{
						if (isFloat)
						{
							if (double.TryParse(str, out var val))
							{
								floatMin = Math.Min(floatMin, val);
								floatMax = Math.Max(floatMax, val);
							}
							else
							{
								isFloat = false;
							}
						}
						if (isInt)
						{
							if (long.TryParse(str, out var val))
							{
								intMin = Math.Min(intMin, val);
								intMax = Math.Max(intMax, val);
							}
							else
							{
								isInt = false;
							}
						}
						if (isDateTime || isDate)
						{
							if (DateTime.TryParse(str, out var val))
							{
								dateMin = val < dateMin ? val : dateMin;
								dateMax = val > dateMax ? val : dateMax;
								if (val.TimeOfDay != TimeSpan.Zero)
									isDate = false;
								if (dateHasFractionalSeconds == false && (val.Ticks % TimeSpan.TicksPerSecond) != 0)
								{
									dateHasFractionalSeconds = true;
								}
							}
							else
							{
								isDate = isDateTime = false;
							}
						}
						if (isGuid)
						{
							if (Guid.TryParse(str, out var val))
							{

							}
							else
							{
								isGuid = false;
							}
						}
					}
				}

				if (str != null)
				{
					var len = str.Length;
					stringLenMax = Math.Max(stringLenMax, len);
					stringLenMin = Math.Min(stringLenMin, len);
					stringLenTotal += len;

					if (len == 0)
						nullCount++;

					if (isAscii)
					{
						foreach (var c in str)
						{
							if (c > 128)
							{
								isAscii = false;
								break;
							}
						}
					}
					
					if (this.valueCount.TryGetValue(str, out int count))
					{
						isUnique = false;
						this.valueCount[str] = count + 1;
					}
					else
					{
						this.valueCount[str] = 1;
					}
				} else
				{
					nullCount++;
				}
			}

			const int DefaultStringSize = 128;

			public DbColumn CreateColumnSchema()
			{
				if(nullCount == count)
				{
					// never saw any values, so no determination could be made
					return ColumnSchema.CreateText(this.ordinal, name, true);
				}

				if (this.isDate || this.isDateTime)
				{
					int? precision = isDate ? (int?)null : dateHasFractionalSeconds ? 7 : 0;
					return ColumnSchema.CreateDate(this.ordinal, name, isNullable, isUnique, precision);
				}

				if (this.isInt)
				{
					var type =
						intMin < int.MinValue || intMax > int.MaxValue
						? typeof(long)
						: typeof(int);
					return ColumnSchema.CreateInt(this.ordinal, name, isNullable, isUnique, type);
				}

				if (this.isFloat)
				{
					var type =
						floatMin < float.MinValue || floatMax > float.MaxValue
						? typeof(double)
						: typeof(float);

					return ColumnSchema.CreateFloat(this.ordinal, name, isNullable, type);
				}

				var len = Math.Max(DefaultStringSize, stringLenMax);
				return ColumnSchema.CreateString(this.ordinal, name, isNullable, isUnique, len, isAscii);
			}
		}
	}
}
