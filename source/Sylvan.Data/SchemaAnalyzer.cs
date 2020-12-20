using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Threading.Tasks;

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

	public sealed partial class SchemaAnalyzer
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

		internal Schema.Column.Builder CreateColumnSchema()
		{
			var name = this.name ?? "";
			if (nullCount == count)
			{
				// never saw any values, so no determination could be made
				return new Schema.Column.Builder(name, typeof(string), true);
			}

			if (this.isBoolean)
			{
				return new Schema.Column.Builder(name, typeof(bool), isNullable);
			}

			if (this.isDate || this.isDateTime)
			{
				int? scale = isDate ? (int?)null : dateHasFractionalSeconds ? 7 : 0;
				return new Schema.Column.Builder(name, typeof(DateTime), isNullable)
				{
					NumericScale = scale
				};
			}

			if (this.isInt)
			{
				var type =
					intMin < int.MinValue || intMax > int.MaxValue
					? typeof(long)
					: typeof(int);
				return new Schema.Column.Builder(name, type, isNullable)
				{
					IsUnique = isUnique
				};
			}

			if (this.isFloat)
			{
				var type =
					floatMin < float.MinValue || floatMax > float.MaxValue
					? typeof(double)
					: typeof(float);
				return new Schema.Column.Builder(name, type, isNullable);
			}

			var len = stringLenMax;
			return new Schema.Column.Builder(name, typeof(string), isNullable)
			{
				ColumnSize = len,
				NumericPrecision = isAscii ? 2 : 1,
				CommonDataType = isAscii ? System.Data.DbType.AnsiString : System.Data.DbType.String,
				IsUnique = isUnique
			};
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
