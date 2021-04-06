using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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


		static readonly Regex SeriesKeyRegex = new Regex(@"\{.+\}");
		// CALLED VIA REFLECTION DO NOT DELETE
		static object GetSeriesAccessor<TK>(Schema.Column seriesCol, ReadOnlyCollection<DbColumn> physicalSchema)
		{
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

			var regex = new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

			var parseMethod = type.GetMethod("TryParse", BindingFlags.Static | BindingFlags.Public, null, new Type[] { typeof(string), type.MakeByRefType() }, null);

			object?[] args = new object[2];
			foreach (var col in physicalSchema)
			{
				var name = col.ColumnName;
				var match = regex.Match(name);
				if (match.Success)
				{
					var keyStr = match.Captures[0].Value;
					args[0] = keyStr;
					args[1] = null;
					var success = (bool)parseMethod.Invoke(null, args);
					if (success)
					{
						var key = (TK)args[1]!;
						var ordinal = col.ColumnOrdinal!.Value;
						cols.Add(new DataSeriesColumn<TK>(name, key, ordinal));
					}
				}
			}

			return Activator.CreateInstance(accessorType, cols)!;
		}
	}
}
