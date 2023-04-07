﻿using System;
using System.Linq;
using System.Reflection;

namespace Sylvan.Data.Csv;

#if SPAN
delegate bool TryParse<T>(ReadOnlySpan<char> str, bool ignoreCase, out T value);
#else
delegate bool TryParse<T>(string str, bool ignoreCase, out T value);
#endif

static class EnumParse
{

#if SPAN
	static readonly Type ParamType = typeof(ReadOnlySpan<char>);
#else
	static readonly Type ParamType = typeof(string);
#endif

	internal static MethodInfo? GenericSpanParseMethod;

	static MethodInfo? GetGenericMethod()
	{
		return
			typeof(Enum)
			.GetMethods()
			.Where(m =>
			{
				if (m.Name != "TryParse")
					return false;
				var p = m.GetParameters();
				return p.Length == 3 && p[0].ParameterType == ParamType;
			}
			)
			.SingleOrDefault();
	}

	static EnumParse()
	{
		GenericSpanParseMethod = GetGenericMethod();
	}
}

sealed class EnumAccessor<T> : IFieldAccessor<T>
{
	internal static EnumAccessor<T> Instance = new();

	internal static TryParse<T>? Parser;

	static EnumAccessor()
	{
		// NOTE: all of this reflection complexity is to avoid boxing in the GetValue method.

		Parser = null;
		var method = EnumParse.GenericSpanParseMethod;
		if (method != null)
		{
			var gm = method.MakeGenericMethod(new[] { typeof(T) });
			Parser = (TryParse<T>)gm.CreateDelegate(typeof(TryParse<T>));
		}
	}

	public T GetValue(CsvDataReader reader, int ordinal)
	{
		var parser = Parser;
		if (parser == null)
		{
			throw new NotSupportedException();
		}
#if SPAN
		var span = reader.GetFieldSpan(ordinal);
#else
		var span = reader.GetString(ordinal);
#endif
		return
			parser(span, true, out T value)
			? value
			: throw new FormatException();
	}
}

sealed class EnumAccessor : IFieldAccessor
{
	readonly Type enumType;

	public EnumAccessor(Type enumType)
	{
		this.enumType = enumType;
	}

	public object GetValueAsObject(CsvDataReader reader, int ordinal)
	{
#if ENUM_SPAN_PARSE
		var span = reader.GetFieldSpan(ordinal);
#else
		var span = reader.GetString(ordinal);
#endif
		return span.Length == 0 
			// the only way we get here is if the schema indicates
			// that this column is non-nullable.
			? throw new FormatException()
			: Enum.Parse(this.enumType, span, ignoreCase: true);
	}
}
