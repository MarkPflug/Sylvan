using System;
using System.Linq;
using System.Reflection;

namespace Sylvan.Data.Csv
{

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
				.Where(m => {
					if (m.Name != "TryParse")
						return false;
					var p = m.GetParameters();
					return p.Count() == 3 && p[0].ParameterType == ParamType;
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
		internal static EnumAccessor<T> Instance = new EnumAccessor<T>();

		internal static TryParse<T>? Parser;

		static EnumAccessor()
		{
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
			if (span.Length == 0) return default!;
			return
				parser(span, true, out T value)
				? value
				: throw new FormatException();
		}
	}
}
