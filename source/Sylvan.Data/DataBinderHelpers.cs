using System;
using System.Data;
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
		//internal readonly static MethodInfo ChangeTypeMethod = GetBinderMethod("ChangeType");
		internal readonly static MethodInfo IsNullStringMethod = GetBinderMethod("IsNullString");

		internal static bool IsNullString(string str)
		{
			return string.IsNullOrWhiteSpace(str);
		}

		//internal static object ChangeType(object value, Type type, CultureInfo culture)
		//{
		//	return Convert.ChangeType(value, type, culture);
		//}

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
	}
}
