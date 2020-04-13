using Sylvan.Data.Csv;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Sylvan.Tools.CsvSchemaBuilder
{
	class Program
	{

		// TOOD: resourceify this?
		const string CommonHeaders = "id,name,code,type,kind,class,date,address,zip,city,state,country,amount,value,text,description,price";

		static void Main(string[] args)
		{
			var file = args[0];
			var (delimiter, quote) = IdentifyFormat(file);
			Console.WriteLine("Delimiter: " + delimiter);
			Console.WriteLine("Quote: " + quote);

			using var tr = File.OpenText(file);
			var dr = CsvDataReader.Create(tr, new CsvDataReaderOptions() { Delimiter = delimiter, Quote = quote });
			WriteHeaders(dr);
			var colInfos = new ColumnInfo[dr.FieldCount];
			for (int i = 0; i < colInfos.Length; i++)
			{
				colInfos[i] = new ColumnInfo(i);
			}
			int count = 1000000;
			int c = 0;
			var sw = Stopwatch.StartNew();
			while (dr.Read() && c++ < count)
			{
				for (int i = 0; i < dr.FieldCount; i++)
				{
					colInfos[i].Analyze(dr, i);
				}
			}

			DbColumn[] columns = new DbColumn[colInfos.Length];
			for (int i = 0; i < colInfos.Length; i++)
			{
				columns[i] = colInfos[i].CreateColumnSchema(dr.GetName(i));
			}
			sw.Stop();
			Console.WriteLine(sw.Elapsed.ToString());
		}

		class ColumnSchema : DbColumn
		{
			public override string ToString()
			{
				var size = this.DataType == typeof(string) ? "[" + this.ColumnSize + "]" : "";
				return $"{this.ColumnName} {this.DataTypeName}{(this.AllowDBNull == true ? "?" : "")}{size}";
			}

			ColumnSchema(int ordinal, string? name, bool isNullable, bool isUnique) {
				this.ColumnOrdinal = ordinal;
				this.ColumnName = name;
				this.AllowDBNull = isNullable;
				this.IsUnique = IsUnique;
			}

			public static ColumnSchema CreateString(int ordinal, string? name, bool isNullable, bool isUnique, int length, bool isAscii) {
				return
					new ColumnSchema(ordinal, name, isNullable, isUnique)
					{
						ColumnSize = length,
						NumericPrecision = isAscii ? 8 : 16,
						DataType = typeof(string),
						DataTypeName = "string",
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

			public static ColumnSchema CreateFloat(int ordinal, string? name, bool isNullable, bool isUnique, Type type)
			{
				var (p, s, n) =
					(type == typeof(double))
					? (15, 8, "double")
					: (7, 4, "float");

				return
					new ColumnSchema(ordinal, name, isNullable, isUnique)
					{
						DataType = type,
						DataTypeName = n,
						NumericPrecision = p,
						ColumnSize = s,
					};
			}
		}

		class ColumnInfo
		{
			public ColumnInfo(int ordinal)
			{
				this.ordinal = ordinal;
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
				isAscii = true;

				this.valueCount = new Dictionary<string, int>();
			}

			bool isAscii;
			int ordinal;
			bool isInt, isFloat, isDate, isDateTime, isGuid;
			bool dateHasFractionalSeconds;
			bool isNullable;
			bool isUnique;

			long intMin, intMax;
			double floatMin, floatMax;
			DateTime dateMin, dateMax;
			int stringLenMax;

			Dictionary<string, int> valueCount;

			public void Analyze(DbDataReader dr, int ordinal)
			{
				var isNull = dr.IsDBNull(ordinal);
				if (isNull)
				{
					this.isNullable = true;
					return;
				}

				var str = dr.GetString(ordinal);
				if (str == null)
					isNullable = true;

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
					stringLenMax = Math.Max(stringLenMax, str.Length);

					if (isAscii)
					{
						foreach(var c in str)
						{
							if(c >= 128)
							{
								isAscii = false;
								break;
							}
						}
					}
					if (isUnique)
					{
						if (this.valueCount.TryGetValue(str, out int count))
						{
							Console.WriteLine(this.ordinal + ": " + str);
							isUnique = false;
							this.valueCount = null;
						}
						else
						{
							this.valueCount[str] = 1;
						}
					}
				}
			}

			public DbColumn CreateColumnSchema(string? name)
			{
				if(this.isDate || this.isDateTime)
				{
					int? precision = isDate ? (int?) null : dateHasFractionalSeconds ? 7 : 0;
					return ColumnSchema.CreateDate(this.ordinal, name, isNullable, isUnique, precision);
				}

				if(this.isInt)
				{
					var type =
						intMin < int.MinValue || intMax > int.MaxValue
						? typeof(long)
						: typeof(int);
					return ColumnSchema.CreateInt(this.ordinal, name, isNullable, isUnique, type);
				}

				if(this.isFloat)
				{
					var type =
						floatMin < float.MinValue || floatMax > float.MaxValue
						? typeof(double)
						: typeof(float);

					return ColumnSchema.CreateFloat(this.ordinal, name, isNullable, isUnique, type);
				}

				return ColumnSchema.CreateString(this.ordinal, name, isNullable, isUnique, stringLenMax, isAscii);
			}
		}
		
		static void WriteHeaders(DbDataReader reader)
		{
			for (int i = 0; i < reader.FieldCount; i++)
			{
				Console.WriteLine(reader.GetName(i));
			}
		}

		static (char delimiter, char quote) IdentifyFormat(string file)
		{
			var tr = File.OpenText(file);
			var buffer = new char[0x100];
			var len = tr.ReadBlock(buffer, 0, buffer.Length);
			var counters = new int[128];
			for (int i = 0; i < len; i++)
			{
				var c = buffer[i];
				if (c > counters.Length)
				{
					continue;
				}
				if (c == '\r' || c == '\n') break;
				counters[c]++;
			}
			var charCounts = counters.Select((c, i) => new { Count = c, Char = (char)i, IsDelim = IsPotentialDelimiter((char)i) });

			var ldCount = charCounts.Where(d => !d.IsDelim);

			var delimiters =
				charCounts
				.Where(d => d.IsDelim)
				.OrderByDescending(d => d.Count)
				.ToArray();

			var delimiter = delimiters.FirstOrDefault()?.Char;

			if (delimiter == null)
				throw new InvalidDataException();

			return (delimiter.Value, '\"');
		}

		static bool IsPotentialDelimiter(char c)
		{
			return c < 128 && char.IsLetterOrDigit(c) == false && c != '_' && c != ' ';
		}
	}
}
