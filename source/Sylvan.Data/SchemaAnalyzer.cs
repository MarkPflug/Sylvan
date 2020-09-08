using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static Sylvan.Data.SchemaAnalyzer;

namespace Sylvan.Data
{
	public sealed class SchemaAnalyzerOptions
	{
		internal static readonly SchemaAnalyzerOptions Default = new SchemaAnalyzerOptions();

		public SchemaAnalyzerOptions()
		{
			this.AnalyzeRowCount = 10000;
			this.ColumnSizer = ColumnSize.Human;
		}

		public int AnalyzeRowCount { get; set; }

		public IColumnSizeStrategy ColumnSizer { get; set; }
	}

	public class ColumnSize
	{
		class ExactColumnSizer : IColumnSizeStrategy
		{
			public int GetSize(int maxLen)
			{
				return maxLen;
			}
		}

		class ProgrammerNumberColumnSizer : IColumnSizeStrategy
		{
			public int GetSize(int maxLen)
			{
				var size = 1;
				while (size < maxLen)
				{
					size = size * 2;
				}
				return size;
			}
		}

		class HumanNumberColumnSizer : IColumnSizeStrategy
		{
			static readonly int[] Sizes = new int[] { 1, 2, 3, 4, 5, 10, 25, 50, 100, 150, 200 };

			public int GetSize(int maxLen)
			{
				if (maxLen > 1000) return ushort.MaxValue;

				var size = 0;
				for (int i = 0; i < Sizes.Length; i++)
				{
					size = Sizes[i];
					if (size > maxLen)
						return size;
				}
				while (size < maxLen)
					size += 100;
				return size;
			}
		}

		class FixedColumnSizer : IColumnSizeStrategy
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

		public static readonly IColumnSizeStrategy Exact = new ExactColumnSizer();
		public static readonly IColumnSizeStrategy Programmer = new ProgrammerNumberColumnSizer();
		public static readonly IColumnSizeStrategy Human = new HumanNumberColumnSizer();

		public static readonly IColumnSizeStrategy Size256 = new FixedColumnSizer(256);
		public static readonly IColumnSizeStrategy Size1024 = new FixedColumnSizer(1024);
	}

	public interface IColumnSizeStrategy
	{
		int GetSize(int maxLen);
	}

	public sealed class SchemaAnalyzer
	{
		int rowCount;
		IColumnSizeStrategy sizer;

		public SchemaAnalyzer(SchemaAnalyzerOptions? options = null)
		{
			options ??= SchemaAnalyzerOptions.Default;
			this.rowCount = options.AnalyzeRowCount;
			this.sizer = options.ColumnSizer;
		}



		public AnalysisResult Analyze(DbDataReader dataReader)
		{
			return AnalyzeAsync(dataReader).GetAwaiter().GetResult();
		}

		public async Task<AnalysisResult> AnalyzeAsync(DbDataReader dataReader)
		{
			var colInfos = new ColumnInfo[dataReader.FieldCount];
			for (int i = 0; i < colInfos.Length; i++)
			{
				colInfos[i] = new ColumnInfo(dataReader, i);
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

		internal class ColumnSchema : DbColumn
		{
			public bool IsIntegerSeries { get; private set; }
			public string? IntegerSeriesFormat { get; private set; }

			public bool IsDateSeries { get; private set; }


			public override object? this[string property]
			{
				get
				{
					switch (property)
					{
						case nameof(IsIntegerSeries):
							return IsIntegerSeries;
						case nameof(IsDateSeries):
							return IsDateSeries;
						case nameof(IntegerSeriesFormat):
							return IntegerSeriesFormat;
						default:
							return base[property];
					}
				}
			}

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
				this.IsUnique = isUnique;
			}

			static (int precision, int scale, string name) GetTypeInfo(Type type)
			{
				return
					(type == typeof(long))
					? (19, 8, "long")
					: (type == typeof(int))
					? (18, 4, "int")
					: (type == typeof(short))
					? (5, 2, "short")
					: (3, 1, "byte");
			}

			internal static ColumnSchema CreateSeries(int ordinal, string? name, bool isNullable, Type type, SeriesType st, string headerFormat)
			{
				var (p, s, n) = GetTypeInfo(type);

				var cs =
					new ColumnSchema(ordinal, name, isNullable, false)
					{
						IsIntegerSeries = st == SeriesType.Integer,
						IntegerSeriesFormat = st == SeriesType.Integer ? headerFormat : null,
						IsDateSeries = st == SeriesType.Date,						
						DataType = type,
						DataTypeName = n,
						NumericPrecision = p,
						ColumnSize = s,
					};

				return cs;
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

			public static ColumnSchema CreateBoolean(int ordinal, string? name, bool isNullable)
			{
				return
					new ColumnSchema(ordinal, name, isNullable, false)
					{
						DataType = typeof(bool)
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

			public static ColumnSchema CreateFloat(int ordinal, string? name, bool isNullable)
			{
				return
					new ColumnSchema(ordinal, name, isNullable, false)
					{
						DataType = typeof(float),
						DataTypeName = "float",
						NumericPrecision = 7,
						ColumnSize = 4,
					};
			}

			public static ColumnSchema CreateDouble(int ordinal, string? name, bool isNullable)
			{
				return
					new ColumnSchema(ordinal, name, isNullable, false)
					{
						DataType = typeof(double),
						DataTypeName = "double",
						NumericPrecision = 15,
						ColumnSize = 8,
					};
			}
		}

		public sealed class ColumnInfo
		{
			internal ColumnInfo(DbDataReader dr, int ordinal)
			{
				this.ordinal = ordinal;
				this.name = dr.GetName(ordinal);
				this.type = dr.GetFieldType(ordinal);

				type = dr.GetFieldType(ordinal);
				var typeCode = Type.GetTypeCode(type);

				switch (typeCode)
				{
					case TypeCode.Boolean:
						isBoolean = true;
						break;
					case TypeCode.Int16:
					case TypeCode.Int32:
					case TypeCode.Int64:
						isInt = true;
						break;
					case TypeCode.Single:
					case TypeCode.Double:
						isFloat = true;
						break;
					case TypeCode.Decimal:
						break;
					case TypeCode.DateTime:
						isDateTime = isDate = true;
						break;
					case TypeCode.String:
						isBoolean = isInt = isFloat = isDate = isDateTime = isGuid = true;
						break;
					default:
						if (type == typeof(byte[]))
						{

						}
						if (type == typeof(Guid))
						{
							isGuid = true;
						}
						break;
				}

				isNullable = false;
				dateHasFractionalSeconds = false;
				isUnique = true;
				intMax = long.MinValue;
				intMin = long.MaxValue;
				floatMax = double.MinValue;
				floatMin = double.MaxValue;
				floatHasFractionalPart = false;
				dateMin = DateTime.MaxValue;
				dateMax = DateTime.MinValue;
				stringLenMax = 0;
				stringLenMin = int.MaxValue;
				stringLenTotal = 0;
				isAscii = true;


				this.valueCount = new Dictionary<string, int>();
			}

			public bool AllowDbNull => isNullable;

			public int Ordinal => this.ordinal;
			public string? Name => this.name;
			Type type;

			bool isAscii;
			int ordinal;
			string? name;
			bool isBoolean, isInt, isFloat, isDate, isDateTime, isGuid;
			bool dateHasFractionalSeconds;
#pragma warning disable CS0414
			bool floatHasFractionalPart;
#pragma warning restore
			bool isNullable;
			bool isUnique;

			int count;
			int nullCount;
			int emptyStringCount;

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

				bool? boolValue = null;
				long? intValue = null;
				double? floatValue = null;
				decimal? decimalValue = null;
				DateTime? dateValue = null;
				string? stringValue = null;
				Guid? guidValue = null;

				var typeCode = Type.GetTypeCode(type);

				switch (typeCode)
				{
					case TypeCode.Boolean:
						boolValue = dr.GetBoolean(ordinal);
						break;
					case TypeCode.Int16:
						intValue = dr.GetInt16(ordinal);
						break;
					case TypeCode.Int32:
						intValue = dr.GetInt32(ordinal);
						break;
					case TypeCode.Int64:
						intValue = dr.GetInt64(ordinal);
						break;
					case TypeCode.Single:
						floatValue = dr.GetFloat(ordinal);
						break;
					case TypeCode.Double:
						floatValue = dr.GetDouble(ordinal);
						break;
					case TypeCode.Decimal:
						decimalValue = dr.GetDecimal(ordinal);
						break;
					case TypeCode.DateTime:
						dateValue = dr.GetDateTime(ordinal);
						break;
					case TypeCode.String:
						stringValue = dr.GetString(ordinal);

						if (isBoolean || isFloat || isInt || isDate || isDateTime || isGuid)
						{
							if (string.IsNullOrEmpty(stringValue))
							{
								isNullable = true;
							}
							else
							{
								if (isBoolean)
								{
									if (bool.TryParse(stringValue, out var b))
									{

									}
									else
									{
										isBoolean = false;
									}
								}
								if (isFloat)
								{
									if (double.TryParse(stringValue, out var f))
									{
										floatValue = f;

									}
									else
									{
										isFloat = false;
									}
								}
								if (isInt)
								{
									if (long.TryParse(stringValue, out var i))
									{
										intValue = i;
									}
									else
									{
										isInt = false;
									}
								}
								if (isDateTime || isDate)
								{
									if (DateTime.TryParse(stringValue, out var d))
									{
										dateValue = d;
									}
									else
									{
										isDate = isDateTime = false;
									}
								}
								if (isGuid)
								{
									if (Guid.TryParse(stringValue, out var g))
									{
										guidValue = g;
									}
									else
									{
										isGuid = false;
									}
								}
							}
						}

						break;
					default:
						if (type == typeof(byte[]))
						{

						}
						if (type == typeof(Guid))
						{
							isGuid = true;
						}
						break;
				}

				if (isFloat && floatValue.HasValue)
				{
					var val = floatValue.Value;
					floatMin = Math.Min(floatMin, val);
					floatMax = Math.Max(floatMax, val);
					if (val != Math.Round(val))
					{
						floatHasFractionalPart = true;
					}
				}

				if (isInt && intValue.HasValue)
				{
					intMin = Math.Min(intMin, intValue.Value);
					intMax = Math.Max(intMax, intValue.Value);
				}

				if (isDateTime && dateValue.HasValue)
				{
					var val = dateValue.Value;
					dateMin = val < dateMin ? val : dateMin;
					dateMax = val > dateMax ? val : dateMax;
					if (isDate && val.TimeOfDay != TimeSpan.Zero)
						isDate = false;
					if (dateHasFractionalSeconds == false && (val.Ticks % TimeSpan.TicksPerSecond) != 0)
					{
						dateHasFractionalSeconds = true;
					}
				}

				if (stringValue != null)
				{
					var len = stringValue.Length;
					stringLenMax = Math.Max(stringLenMax, len);
					stringLenMin = Math.Min(stringLenMin, len);
					stringLenTotal += len;
					if (len == 0)
					{
						emptyStringCount++;
					}

					if (isAscii)
					{
						foreach (var c in stringValue)
						{
							if (c >= 128)
							{
								isAscii = false;
								break;
							}
						}
					}

					if (this.valueCount.TryGetValue(stringValue, out int count))
					{
						isUnique = false;
						this.valueCount[stringValue] = count + 1;
					}
					else
					{
						this.valueCount[stringValue] = 1;
					}
				}
			}

			const int DefaultStringSize = 128;

			[Flags]
			internal enum ColType
			{
				None = 0,
				Boolean = 1,
				Date = 2,
				Integer = 4,
				Long = 8,
				Float = 16,
				Double = 32,
				Decimal = 32,
				String = 128,
			}

			static Type[] ColTypes = new[]
			{
				typeof(bool),
				typeof(DateTime),
				typeof(int),
				typeof(long),
				typeof(float),
				typeof(double),
				typeof(decimal),
				typeof(string),
			};

			internal ColType GetColType()
			{
				if (nullCount == count)
				{
					// never saw any values, so no determination could be made
					return ColType.None;
				}

				if (this.isBoolean)
				{
					return ColType.Boolean;
				}

				if (this.isDate || this.isDateTime)
				{
					return ColType.Date;
				}

				if (this.isInt)
				{
					var type =
						intMin < int.MinValue || intMax > int.MaxValue
						? ColType.Long | ColType.Float | ColType.Double | ColType.Decimal
						: ColType.Integer | ColType.Long | ColType.Float | ColType.Double | ColType.Decimal;

					return type;
				}

				if (this.isFloat)
				{
					return
						floatMin < float.MinValue || floatMax > float.MaxValue
						? ColType.Double | ColType.Decimal
						: ColType.Float | ColType.Double | ColType.Decimal;
				}

				return ColType.String;
			}

			internal static Type GetType(ColType t)
			{
				for (int i = 1; i < 16; i++)
				{
					var flag = (ColType)(1 << i);
					if ((flag & t) == flag)
					{
						return ColTypes[i];
					}
				}
				return typeof(string);
			}

			internal ColumnSchema CreateColumnSchema()
			{
				if (nullCount == count)
				{
					// never saw any values, so no determination could be made
					return ColumnSchema.CreateText(this.ordinal, name, true);
				}

				if (this.isBoolean)
				{
					return ColumnSchema.CreateBoolean(this.ordinal, name, isNullable);
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
					return
						floatMin < float.MinValue || floatMax > float.MaxValue
						? ColumnSchema.CreateDouble(this.ordinal, name, isNullable)
						: ColumnSchema.CreateFloat(this.ordinal, name, isNullable);
				}

				var len = stringLenMax;
				return ColumnSchema.CreateString(this.ordinal, name, isNullable, isUnique, len, isAscii);
			}
		}
	}

	public class AnalysisResult : IEnumerable<ColumnInfo>
	{
		readonly ColumnInfo[] columns;
		//SchemaAnalyzerOptions options;

		internal AnalysisResult(/*SchemaAnalyzerOptions options,*/ ColumnInfo[] columns)
		{
			//this.options = options;
			this.columns = columns;
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

		public ReadOnlyCollection<DbColumn> GetSchema()
		{
			var series = DetectSeries(columns);
			var schema = new List<DbColumn>();
			for (int i = 0; i < columns.Length; i++)
			{
				var col = columns[i];

				if (series?.seriesStart == i)
				{
					int idx = i;
					string? prefix = series.prefix;
					var types = col.GetColType();
					var allowNull = false;
					for (; i <= series.seriesEnd; i++)
					{
						col = columns[i];
						allowNull |= col.AllowDbNull;
						types &= col.GetColType();
					}
					var type = ColumnInfo.GetType(types);
					var columnSchema = ColumnSchema.CreateSeries(idx, "*", allowNull, type, series.type, prefix + "{0}");
					i = series.seriesEnd;
					schema.Add(columnSchema);
					continue;
				}
				var dbCol = col.CreateColumnSchema();
				schema.Add(dbCol);
			}
			return new ReadOnlyCollection<DbColumn>(schema);
		}



		class SeriesInfo
		{
			public SeriesInfo(int idx)
			{
				this.seriesStart = idx;
				this.seriesEnd = idx;
			}

			public SeriesType type;
			public string? prefix;
			public int value = -1;
			public int step;
			public int seriesStart;
			public int seriesEnd;
		}

		SeriesInfo? DetectSeries(ColumnInfo[] cols)
		{
			var series = new SeriesInfo[cols.Length];

			SeriesInfo? ss = null;

			for (int i = 0; i < this.columns.Length; i++)
			{
				var s = series[i] = new SeriesInfo(i);

				var col = this.columns[i];
				var name = col.Name;
				if (name == null) continue;

				if (DateTime.TryParse(name, out var _))
				{
					s.type |= SeriesType.Date;
				}
				else
				{
					var match = Regex.Match(name, @"\d+$");
					if (match.Success)
					{
						var prefix = name.Substring(0, name.Length - match.Length);
						s.prefix = prefix;
						s.value = int.Parse(match.Captures[0].Value);
						s.type |= SeriesType.Integer;
					}
				}

				if (i > 0 && s.type != SeriesType.None)
				{
					var prev = series[i - 1];
					var step = s.value - prev.value;
					if (prev.type == s.type && StringComparer.InvariantCultureIgnoreCase.Equals(prev.prefix, s.prefix))
					{
						s.seriesStart = prev.seriesStart;
						ss = ss ?? prev;
						s.step = step;
						series[s.seriesStart].seriesEnd = i;
					}
				}
			}
			return ss;
		}
	}

	[Flags]
	enum SeriesType
	{
		None = 0,
		Integer = 1,
		Date = 2,
	}
}
