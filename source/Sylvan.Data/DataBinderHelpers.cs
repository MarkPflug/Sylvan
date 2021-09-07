using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Sylvan.Data
{
	partial class DataBinder
	{
		internal static string? MapName(string columnName, int ordinal)
		{
			var name = Regex.Replace(columnName, "[^A-Za-z0-9_]", "");
			if (name.Length == 0)
			{
				return "Column" + ordinal;
			}
			else
			{
				if (char.IsDigit(name[0]))
				{
					name = "Column" + name;
				}
			}
			return name;
		}

		internal static readonly Type IDataRecordType = typeof(IDataRecord);

		internal static readonly MethodInfo IsDbNullMethod;
		internal static readonly MethodInfo GetBooleanMethod;
		internal static readonly MethodInfo GetCharMethod;
		internal static readonly MethodInfo GetByteMethod;
		internal static readonly MethodInfo GetInt16Method;
		internal static readonly MethodInfo GetInt32Method;
		internal static readonly MethodInfo GetInt64Method;
		internal static readonly MethodInfo GetFloatMethod;
		internal static readonly MethodInfo GetDoubleMethod;
		internal static readonly MethodInfo GetDecimalMethod;
		internal static readonly MethodInfo GetGuidMethod;
		internal static readonly MethodInfo GetStringMethod;
		internal static readonly MethodInfo GetDateTimeMethod;
		internal static readonly MethodInfo GetValueMethod;

		static DataBinder()
		{
			IDataRecordType = typeof(IDataRecord);
			IsDbNullMethod = IDataRecordType.GetMethod("IsDBNull")!;
			GetBooleanMethod = IDataRecordType.GetMethod("GetBoolean");
			GetCharMethod = IDataRecordType.GetMethod("GetChar");
			GetByteMethod = IDataRecordType.GetMethod("GetByte");
			GetInt16Method = IDataRecordType.GetMethod("GetInt16");
			GetInt32Method = IDataRecordType.GetMethod("GetInt32");
			GetInt64Method = IDataRecordType.GetMethod("GetInt64");
			GetFloatMethod = IDataRecordType.GetMethod("GetFloat");
			GetDoubleMethod = IDataRecordType.GetMethod("GetDouble");
			GetDecimalMethod = IDataRecordType.GetMethod("GetDecimal");
			GetStringMethod = IDataRecordType.GetMethod("GetString");
			GetGuidMethod = IDataRecordType.GetMethod("GetGuid");
			GetDateTimeMethod = IDataRecordType.GetMethod("GetDateTime");

			GetValueMethod = IDataRecordType.GetMethod("GetValue");
		}

		internal static MethodInfo? GetAccessorMethod(Type type)
		{
			switch (Type.GetTypeCode(type))
			{
				case TypeCode.Boolean:
					return GetBooleanMethod;
				case TypeCode.Byte:
					return GetByteMethod;
				case TypeCode.Int16:
				case TypeCode.SByte:
					return GetInt16Method;
				case TypeCode.Int32:
				case TypeCode.UInt16:
					return GetInt32Method;
				case TypeCode.Int64:
				case TypeCode.UInt32:
					return GetInt64Method;
				case TypeCode.DateTime:
					return GetDateTimeMethod;
				case TypeCode.Char:
					return GetCharMethod;
				case TypeCode.String:
				case TypeCode.UInt64:
					return GetStringMethod;
				case TypeCode.Single:
					return GetFloatMethod;
				case TypeCode.Double:
					return GetDoubleMethod;
				case TypeCode.Decimal:
					return GetDecimalMethod;
				default:
					if (type == typeof(Guid))
					{
						return GetGuidMethod;
					}

					if (type == typeof(byte[]) || type == typeof(char[]))
					{
						return GetValueMethod;
					}
					break;
			}

			return null;
		}

		static MethodInfo GetBinderMethod(string name)
		{
			return typeof(DataBinder).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)!;
		}

		internal readonly static MethodInfo EnumParseMethod = GetBinderMethod("ParseEnum");
		internal readonly static MethodInfo IsNullStringMethod = GetBinderMethod("IsNullString");

		internal static bool IsNullString(string str)
		{
			return string.IsNullOrWhiteSpace(str);
		}

		internal static TE ParseEnum<TE>(string value) where TE : struct, Enum
		{
			if (Enum.TryParse(value, true, out TE e))
			{
				return e;
			}
			else
			{
				throw new InvalidEnumValueDataBinderException(typeof(TE), value);
			}
		}

		internal static readonly Regex SeriesKeyRegex = new Regex(@"\{(.+)\}");

		interface IOption<out T>
		{
			public bool Success { get; }
			public T? Value { get; }
		}

		class Option<T> : IOption<T>
		{
			public bool b;
			public T? v;

			public Option(bool b, T? v)
			{
				this.b = b;
				this.v = v;
			}

			public bool Success => b;
			public T? Value => v;
		}

		// CALLED VIA REFLECTION DO NOT DELETE
		// identifies all the columns that belong to a series and determines the series key for
		// each column and produces a concrete DataSeriesAccessor<,> 
		static object GetSeriesAccessor<TK>(Schema.Column seriesCol, IEnumerable<DbColumn> physicalSchema, out IEnumerable<DbColumn> boundColumns)
		{
			var boundCols = new List<DbColumn>();
			boundColumns = boundCols;

			var fmt = seriesCol.SeriesHeaderFormat; //asdf{Date}qwer => ^asdf(.+)qwer$

			var type = typeof(TK);
			var accessorType = typeof(DataSeriesAccessor<,>).MakeGenericType(type, seriesCol.DataType!);
			var cols = new List<DataSeriesColumn<TK>>();

			var prefix = string.Empty;
			var suffix = string.Empty;

			if (fmt != null)
			{
				var match = SeriesKeyRegex.Match(fmt);
				if (match.Success)
				{
					var start = match.Index;
					prefix = fmt.Substring(0, start);
					suffix = fmt.Substring(start + match.Length);
				}
			}

			var pattern = "^" + Regex.Escape(prefix) + "(.+)" + Regex.Escape(suffix) + "$";
			Func<string, IOption<TK>> f;
			object?[] args = new object[2];
			if (type == typeof(string))
			{
				f = (s) => (IOption<TK>)new Option<string>(true, s);
			}
			else
			{
				var parseMethod = type.GetMethod("TryParse", BindingFlags.Static | BindingFlags.Public, null, new Type[] { typeof(string), type.MakeByRefType() }, null);
				
				f = (s) =>
				{
					var parse = parseMethod;
					args[0] = s;
					args[1] = null;
					var success = (bool)parse.Invoke(null, args);
					if (success)
					{
						return new Option<TK>(true, (TK)args[1]!);
					}
					return new Option<TK>(false, default);
				};				
			}

			var regex = new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
			
			foreach (var col in physicalSchema)
			{
				var name = col.ColumnName;
				var match = regex.Match(name);
				if (match.Success)
				{
					var keyStr = match.Captures[0].Value;
					var r = f(keyStr);
					if (r.Success)
					{
						var ordinal = col.ColumnOrdinal!.Value;
						cols.Add(new DataSeriesColumn<TK>(name, r.Value!, ordinal));
						boundCols.Add(col);
					}
				}
			}

			return Activator.CreateInstance(accessorType, cols)!;
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
				case DbType.DateTimeOffset:
					return typeof(DateTimeOffset);
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

		public static T GetRecord<T>(this IDataBinder<T> binder, IDataRecord record, Func<IDataRecord, Exception, bool>? errorHandler = null) where T : new()
		{
			var t = new T();
			try
			{
				binder.Bind(record, t);
			}
			catch (Exception e) when (errorHandler != null)
			{
				if (!errorHandler(record, e))
				{
					throw;
				}
			}
			return t;
		}

		public static IDataSeriesRange<TK>? GetSeriesRange<TK>(this IDataBinder binder, string seriesName)
		{
			if (binder is IDataSeriesBinder b)
			{
				var acc = b.GetSeriesAccessor(seriesName);
				if (acc != null)
				{
					return (IDataSeriesRange<TK>)acc;
				}
			}
			return null;
		}

		//public static IEnumerable? GetSeriesRange(this IDataBinder binder, string seriesName)
		//{
		//	seriesName = seriesName ?? "";
		//	if (binder is IDataSeriesBinder b)
		//	{
		//		var acc = b.GetSeriesAccessor(seriesName);
		//		return (IEnumerable?)acc;
		//	}
		//	return null;
		//}
	}
}
