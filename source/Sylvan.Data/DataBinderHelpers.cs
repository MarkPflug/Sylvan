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
		internal static string? DefaultNameMapping(string columnName, int ordinal)
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

		static readonly Type drType = typeof(IDataRecord);

		internal static MethodInfo? GetAccessorMethod(Type type)
		{
			switch (Type.GetTypeCode(type))
			{
				case TypeCode.Boolean:
					return drType.GetMethod("GetBoolean")!;
				case TypeCode.Byte:
					return drType.GetMethod("GetByte")!;
				case TypeCode.Int16:
					return drType.GetMethod("GetInt16")!;
				case TypeCode.Int32:
					return drType.GetMethod("GetInt32")!;
				case TypeCode.Int64:
					return drType.GetMethod("GetInt64")!;
				case TypeCode.DateTime:
					return drType.GetMethod("GetDateTime")!;
				case TypeCode.Char:
					return drType.GetMethod("GetChar")!;
				case TypeCode.String:
					return drType.GetMethod("GetString")!;
				case TypeCode.Single:
					return drType.GetMethod("GetFloat")!;
				case TypeCode.Double:
					return drType.GetMethod("GetDouble")!;
				case TypeCode.Decimal:
					return drType.GetMethod("GetDecimal")!;
				default:
					if (type == typeof(Guid))
					{
						return drType.GetMethod("GetGuid")!;
					}

					if (type == typeof(byte[]) || type == typeof(char[]))
					{
						return drType.GetMethod("GetValue");
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
