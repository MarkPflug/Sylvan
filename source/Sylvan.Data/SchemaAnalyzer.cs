using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Sylvan.Data;

/// <summary>
/// Options for a data schema analysis operation.
/// </summary>
public sealed class SchemaAnalyzerOptions
{
	internal static readonly SchemaAnalyzerOptions Default = new();

	/// <summary>
	/// Creates a new SchemaAnalyzerOptions with the default values.
	/// </summary>
	public SchemaAnalyzerOptions()
	{
		this.AnalyzeRowCount = 10000;
		//this.ColumnSizer = ColumnSize.Human;
		this.DetectSeries = false;
		this.TrueStrings = new() { "y", "yes", "t", "true" };
		this.FalseStrings = new() { "n", "no", "f", "false" };
		DateOnlyTimeOnlyTimespanUsage = DateTimeTimespanDateOnlyTimeOnlyUsageOptions.DateTimeAndString;
	}

	/// <summary>
	/// The number of rows to analyze.
	/// </summary>
	public int AnalyzeRowCount { get; set; }

	/// <summary>
	/// Indicates if series detection should be enabled.
	/// </summary>
	public bool DetectSeries { get; set; }

	//public IColumnSizeStrategy ColumnSizer { get; set; }

	/// <summary>
	/// Values to be interpreted as boolean true.
	/// Default values are "y", "yes", "t", "true".
	/// </summary>
	public List<string> TrueStrings { get; set; }
	/// <summary>
	/// Values to be interpreted as boolean true.
	/// Default values are "n", "no", "f", "false".
	/// </summary>
	public List<string> FalseStrings { get; set; }

	/// <summary>
	/// Set the preference order for usage of DateTime, Timespan, DateOnly and/or TimeOnly
	/// </summary>
	public DateTimeTimespanDateOnlyTimeOnlyUsageOptions DateOnlyTimeOnlyTimespanUsage { get; set; }
}

/// <summary>
/// Options to preference different data types during the analysis
/// </summary>
public enum DateTimeTimespanDateOnlyTimeOnlyUsageOptions
{
	/// <summary>
	/// This is the original behaviour
	/// Assesses time only values greater than 24 hours as string, time values less than 24 hours as DateTime and dates/date times as DateTime.
	/// i.e. 48:12:23 assessed as string, 12:00:00 assessed as DateTime, 01-01-2020 or 01-01-2020 12:00:00 or 2016-12-23T08:57:21.649 assessed as DateTime.
	/// </summary>
	DateTimeAndString,

	/// <summary>
	/// Assesses time only values as Timespan and dates/date times as DateTime
	/// i.e. 12:00:00 or 48:12:23 assessed as Timespan, 01-01-2020 or 01-01-2020 12:00:00 or 2016-12-23T08:57:21.649 assessed as DateTime.
	/// </summary>
	TimespanAndDateTime,

#if NET6_0_OR_GREATER
	/// <summary>
	/// Assess TimeOnly or DateOnly types rather than DateTime.
	/// i.e. 12:00:00 assessed as TimeOnly, 01-01-2020 assessed as DateOnly, 48:12:23 assessed as Timespan, 01-01-2020 12:00:00 or 2016-12-23T08:57:21.649 assessed as DateTime.
	/// </summary>
	TimeOnlyAndDateOnlyOverTimespanAndDateTime
#endif
}

//public class ColumnSize
//{
//	class ExactColumnSizer : IColumnSizeStrategy
//	{
//		public int GetSize(int maxLen)
//		{
//			return maxLen;
//		}
//	}

//	class ProgrammerNumberColumnSizer : IColumnSizeStrategy
//	{
//		public int GetSize(int maxLen)
//		{
//			var size = 1;
//			while (size < maxLen)
//			{
//				size = size * 2;
//			}
//			return size;
//		}
//	}

//	class HumanNumberColumnSizer : IColumnSizeStrategy
//	{
//		static readonly int[] Sizes = new int[] { 1, 2, 3, 4, 5, 10, 25, 50, 100, 150, 200 };

//		public int GetSize(int maxLen)
//		{
//			if (maxLen > 1000) return ushort.MaxValue;

//			var size = 0;
//			for (int i = 0; i < Sizes.Length; i++)
//			{
//				size = Sizes[i];
//				if (size > maxLen)
//					return size;
//			}
//			while (size < maxLen)
//				size += 100;
//			return size;
//		}
//	}

//	class FixedColumnSizer : IColumnSizeStrategy
//	{
//		readonly int size;
//		public FixedColumnSizer(int size)
//		{
//			this.size = size;
//		}

//		public int GetSize(int maxLen)
//		{
//			return size;
//		}
//	}

//	public static readonly IColumnSizeStrategy Exact = new ExactColumnSizer();
//	public static readonly IColumnSizeStrategy Programmer = new ProgrammerNumberColumnSizer();
//	public static readonly IColumnSizeStrategy Human = new HumanNumberColumnSizer();

//	public static readonly IColumnSizeStrategy Size256 = new FixedColumnSizer(256);
//	public static readonly IColumnSizeStrategy Size1024 = new FixedColumnSizer(1024);
//}

//public interface IColumnSizeStrategy
//{
//	int GetSize(int maxLen);
//}

/// <summary>
/// Analyzes weakly-typed string data to determine schema information.
/// </summary>
public sealed partial class SchemaAnalyzer
{
	readonly int rowCount;
	//IColumnSizeStrategy sizer;
	readonly bool detectSeries;
	readonly IReadOnlyCollection<string>? trueStrings;
	readonly IReadOnlyCollection<string>? falseStrings;
	readonly DateTimeTimespanDateOnlyTimeOnlyUsageOptions dateOnlyTimeOnlyTimespanUsage;

	/// <summary>
	/// Creates a new SchemaAnalyzer.
	/// </summary>
	public SchemaAnalyzer(SchemaAnalyzerOptions? options = null)
	{
		options ??= SchemaAnalyzerOptions.Default;
		this.rowCount = options.AnalyzeRowCount;
		//this.sizer = options.ColumnSizer;
		this.detectSeries = options.DetectSeries;
		this.trueStrings = options.TrueStrings;
		this.falseStrings = options.FalseStrings;
		this.dateOnlyTimeOnlyTimespanUsage = options.DateOnlyTimeOnlyTimespanUsage;
	}

	/// <summary>
	/// Analyzes a data set.
	/// </summary>
	public AnalysisResult Analyze(DbDataReader dataReader)
	{
		return AnalyzeAsync(dataReader).GetAwaiter().GetResult();
	}

	/// <summary>
	/// Analyzes a data set.
	/// </summary>
	public async Task<AnalysisResult> AnalyzeAsync(DbDataReader dataReader)
	{
		var colInfos = new ColumnInfo[dataReader.FieldCount];
		for (int i = 0; i < colInfos.Length; i++)
		{
			colInfos[i] = new ColumnInfo(dataReader, i)
			{
				TrueStrings = trueStrings,
				FalseStrings = falseStrings,
				DateTimeDateOnlyTimeOnlyTimespanUsage = dateOnlyTimeOnlyTimespanUsage
			};
		}

		// Commented out the StopWatch as it looks like it may have been used for performance testing and hadn't been removed.
		//var sw = Stopwatch.StartNew();
		int c = 0;
		while (await dataReader.ReadAsync().ConfigureAwait(false) && c++ < rowCount)
		{
			for (int i = 0; i < dataReader.FieldCount; i++)
			{
				colInfos[i].Analyze(dataReader, i);
			}
		}
		//sw.Stop();

		return new AnalysisResult(detectSeries, colInfos);
	}
}

/// <summary>
/// Schema analysis information for a data column.
/// </summary>
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
				isDecimal = true;
				break;
			case TypeCode.DateTime:
				isDateTime = isDate = true;
				break;
			case TypeCode.String:
				isBoolean = isInt = isFloat = isDecimal = isDate = isDateTime = isTimeSpan = isGuid = true;
#if NET6_0_OR_GREATER
				isDateOnly = isTimeOnly = true;
#endif
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
		intMax = long.MinValue;
		intMin = long.MaxValue;
		floatMax = double.MinValue;
		floatMin = double.MaxValue;
		decimalMin = decimal.MaxValue;
		decimalMax = decimal.MinValue;
		decimalScaleMin = int.MaxValue;
		decimalScaleMax = int.MinValue;
		floatHasFractionalPart = false;
		dateMin = DateTime.MaxValue;
		dateMax = DateTime.MinValue;
		stringLenMax = 0;
		stringLenMin = int.MaxValue;
		stringLenTotal = 0;
		isAscii = true;

		this.valueCount = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
	}

	/// <summary>
	/// Indicates if the column allows null values.
	/// </summary>
	public bool AllowDbNull => isNullable;

	/// <summary>
	/// Gets the column oridnal.
	/// </summary>
	public int Ordinal => this.ordinal;

	/// <summary>
	/// Gets the column name.
	/// </summary>
	public string? Name => this.name;

	readonly Type type;

	bool isAscii;
	readonly int ordinal;
	readonly string? name;
	bool isBoolean, isInt, isFloat, isDate, isDateTime, isTimeSpan, isGuid;
#if NET6_0_OR_GREATER
	bool isDateOnly, isTimeOnly;
#endif

	bool isDecimal;
	bool dateHasFractionalSeconds;
#pragma warning disable CS0414
	bool floatHasFractionalPart;
#pragma warning restore
	bool isNullable;

	int count;
	int nullCount;
	int emptyStringCount;

	long intMin, intMax;
	double floatMin, floatMax;
	decimal decimalMin, decimalMax;
	int decimalScaleMin, decimalScaleMax;
	DateTime dateMin, dateMax;
	int stringLenMax;
	int stringLenMin;
	long stringLenTotal;
	int errorCount = 0;

	readonly Dictionary<string, int> valueCount;

	DateTime? dateValue;
	TimeSpan? timeSpanValue;

	internal void Analyze(DbDataReader dr, int ordinal)
	{
		dateValue = null;
		timeSpanValue = null;
		count++;
		var isNull = dr.IsDBNull(ordinal);
		if (isNull)
		{
			nullCount++;
			this.isNullable = true;
			return;
		}

		bool? boolValue;
		long? intValue = null;
		double? floatValue = null;
		decimal? decimalValue = null;

#if NET6_0_OR_GREATER
		DateOnly? dateOnlyValue = null;
		TimeOnly? timeOnlyValue = null;
#endif
		string? stringValue = null;
		Guid? guidValue;

		var typeCode = Type.GetTypeCode(type);

		try
		{
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
							if (isDecimal)
							{
								if (decimal.TryParse(stringValue, out var d))
								{
									decimalValue = d;
								}
								else
								{
									isDecimal = false;
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
								switch (DateTimeDateOnlyTimeOnlyTimespanUsage)
								{
									case DateTimeTimespanDateOnlyTimeOnlyUsageOptions.DateTimeAndString:
										AssessDateTime(stringValue);
										break;
									case DateTimeTimespanDateOnlyTimeOnlyUsageOptions.TimespanAndDateTime:
										AssessDateTimeAndTimeSpan(stringValue);
										break;
#if NET6_0_OR_GREATER
									case DateTimeTimespanDateOnlyTimeOnlyUsageOptions.TimeOnlyAndDateOnlyOverTimespanAndDateTime:
										bool containsDateSeparators = dateSeparators.Any(sep => stringValue.Contains(sep));
										bool containsTimeSeparators = timeSeparators.Any(sep => stringValue.Contains(sep));
										if (containsDateSeparators && !containsTimeSeparators && DateOnly.TryParse(stringValue, out DateOnly dateOnlyResult))
										{
											// Ensure the string does not contain time components and follows common date formats
											dateOnlyValue = dateOnlyResult;
											isTimeOnly = isDateTime = isTimeSpan = false;
										}
										else if (!containsDateSeparators && containsTimeSeparators && TimeOnly.TryParse(stringValue, out TimeOnly timeOnlyResult))
										{
											// Ensure the string does not contain date components
											timeOnlyValue = timeOnlyResult;
											isDateOnly = isDateTime = isTimeSpan = false;
										}
										else
										{
											AssessDateTimeAndTimeSpan(stringValue);
										}
										break;
#endif
									default:
										throw new NotSupportedException("DateTimeTimespanDateOnlyTimeOnlyUsageOption not supported.");
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
		}
		catch (Exception)
		{
			errorCount++;
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

		if (isDecimal && decimalValue.HasValue)
		{
			var dec = decimalValue.Value;

			decimalMin = Math.Min(decimalMin, dec);
			decimalMax = Math.Max(decimalMax, dec);
			var scale = GetScale(dec);
			decimalScaleMin = Math.Min(decimalScaleMin, scale);
			decimalScaleMax = Math.Max(decimalScaleMax, scale);
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
			if (len == 0 || string.IsNullOrWhiteSpace(stringValue))
			{
				nullCount++;
				emptyStringCount++;
			}
			else
			{
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

				if (this.valueCount.Count < 100)
				{
					if (this.valueCount.TryGetValue(stringValue, out int count))
					{
						this.valueCount[stringValue] = count + 1;
					}
					else
					{
						if (!string.IsNullOrWhiteSpace(stringValue))
						{
							this.valueCount[stringValue] = 1;
						}
					}
				}
			}
		}
	}

	void AssessDateTimeAndTimeSpan(in string stringValue)
	{
		if ((!int.TryParse(stringValue, out var _)) && TimeSpan.TryParse(stringValue, out var timeSpanResult1))
		{
			timeSpanValue = timeSpanResult1;
			isDateTime = false;
#if NET6_0_OR_GREATER
			isDateOnly = isTimeOnly = false;
#endif
		}
		else
		{
			AssessDateTime(stringValue);
		}
	}

	void AssessDateTime(in string stringValue)
	{
		if (DateTime.TryParse(stringValue, out var d1))
		{
			dateValue = d1;
			isTimeSpan = false;
#if NET6_0_OR_GREATER
			isDateOnly = isTimeOnly = false;
#endif
		}
		else
		{
			isDate = isDateTime = isTimeSpan = false;
#if NET6_0_OR_GREATER
			isDateOnly = isTimeOnly = false;
#endif
		}
	}

	static int GetScale(decimal d)
	{
		return new DecimalScale(d).Scale;
	}

	[StructLayout(LayoutKind.Explicit)]
	struct DecimalScale
	{
		public DecimalScale(decimal value)
		{
			this = default;
			this.d = value;
		}

		[FieldOffset(0)]
		readonly decimal d;

		[FieldOffset(0)]
		readonly int flags;

		public int Scale => (flags >> 16) & 0xff;
	}

	[Flags]
	internal enum ColType
	{
		None = 0,
		Boolean = 1,
		Date = 2,
		Integer = 4,
		Long = 8,
		//Float = 16,
		Double = 32,
		Decimal = 64,
		String = 128,
	}

	static readonly Type[] ColTypes = new[]
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
				? ColType.Long | ColType.Double | ColType.Decimal
				: ColType.Integer | ColType.Long | ColType.Double | ColType.Decimal;

			return type;
		}

		if (this.isFloat)
		{
			return ColType.Double | ColType.Decimal;
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
#if NET6_0_OR_GREATER
		if (this.isDateOnly)
		{
			return new Schema.Column.Builder(name, typeof(DateOnly), isNullable);
		}
		if (this.isTimeOnly)
		{
			return new Schema.Column.Builder(name, typeof(TimeOnly), isNullable);
		}
#endif

		if (this.isTimeSpan)
		{
			return new Schema.Column.Builder(name, typeof(TimeSpan), isNullable)
			{
			};
		}
		if (this.isDate || this.isDateTime)
		{
			int? scale = isDate ? (int?)null : dateHasFractionalSeconds ? 7 : 0;
			return new Schema.Column.Builder(name, typeof(DateTime), isNullable)
			{
				NumericScale = scale
			};
		}

		if (isInt)
		{
			if (intMin == 0 && intMax == 1 && count > 2)
			{
				// no format needed, will just parse as int
				// TODO: rethink this, perhaps a format string would be slightly faster.
				return new Schema.Column.Builder(name, typeof(bool), isNullable);
			}
			var type =
				intMin < int.MinValue || intMax > int.MaxValue
				? typeof(long)
				: typeof(int);
			return new Schema.Column.Builder(name, type, isNullable);
		}

		if (isFloat)
		{
			if (isDecimal)
			{
				if (/*IsDecimalHeader(this.name) ||*/ this.IsDecimalRange())
				{
					return new Schema.Column.Builder(name, typeof(decimal), isNullable);
				}
			}

			// never bother with float, even if it appears it would suffice.
			return new Schema.Column.Builder(name, typeof(double), isNullable);
		}

		var len = stringLenMax;
		// check to see if this might be a boolean
		if (len < 6 && valueCount.Count <= 2)
		{
			string? t = null, f = null;
			if (TrueStrings != null)
			{
				foreach (var s in TrueStrings)
				{
					if (valueCount.ContainsKey(s))
					{
						t = s;
						break;
					}
				}
			}
			if (FalseStrings != null)
			{
				foreach (var s in FalseStrings)
				{
					if (valueCount.ContainsKey(s))
					{
						f = s;
						break;
					}
				}
			}
			if (t != null || f != null)
			{
				return new Schema.Column.Builder(name, typeof(bool), isNullable || emptyStringCount > 0)
				{
					CommonDataType = System.Data.DbType.Boolean,
					Format = t + "|" + f,
				};
			}
		}

		return new Schema.Column.Builder(name, typeof(string), isNullable)
		{
			ColumnSize = len,
			NumericPrecision = isAscii ? 2 : 1,
			CommonDataType = isAscii ? System.Data.DbType.AnsiString : System.Data.DbType.String,
		};
	}

	bool IsDecimalRange()
	{
		return this.decimalScaleMax <= 6;
	}

	// TODO: consider localization?
	internal IReadOnlyCollection<string>? TrueStrings;
	internal IReadOnlyCollection<string>? FalseStrings;
	internal DateTimeTimespanDateOnlyTimeOnlyUsageOptions DateTimeDateOnlyTimeOnlyTimespanUsage;
	readonly char[] dateSeparators = new[] { '-', '/', '.' };
	readonly char[] timeSeparators = new[] { ':', 'T' };

	//static bool IsDecimalHeader(string? name)
	//{
	//	if (name == null) return false;
	//	var clean = Regex.Replace(name, "[^a-z]", " ", RegexOptions.IgnoreCase);
	//	foreach (var str in DecimalStrings)
	//	{
	//		if (clean.IndexOf(str, StringComparison.OrdinalIgnoreCase) >= 0)
	//		{
	//			return true;
	//		}
	//	}
	//	return false;
	//}

	// look for these strings in the header to try to make a determination about appropriate type
	//static string[] DecimalStrings = new[] { "amount", "revenue", "price", "cost" };
	//static string[] DoubleStrings = new[] { "lat", "long", "elev", "length", "area", "volume",  };
}

[Flags]
enum SeriesType
{
	None = 0,
	Integer = 1,
	Date = 2,
}