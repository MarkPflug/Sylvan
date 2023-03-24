using System;
using System.Data.Common;
using System.Globalization;

namespace Sylvan.Data
{
	sealed class DynamicDataReader : DataReaderAdapter
	{
		readonly CultureInfo culture;

		public DynamicDataReader(DbDataReader dr, CultureInfo? culture = null) : base(dr)
		{
			this.culture = culture ?? CultureInfo.InvariantCulture;
		}

		public override Type GetFieldType(int ordinal)
		{
			var t = base.GetFieldType(ordinal);
			return t == typeof(string) ? typeof(object) : t;
		}

		public override object GetValue(int ordinal)
		{
			var t = base.GetFieldType(ordinal);

			if (t == typeof(string))
			{
				var str = this.GetString(ordinal);
				if (int.TryParse(str, NumberStyles.Any, culture, out var i32Val))
				{
					return i32Val;
				}

				if (decimal.TryParse(str, NumberStyles.Any, culture, out var decVal))
				{
					return decVal;
				}

				if (DateTime.TryParse(str, culture, DateTimeStyles.None, out var dateVal))
				{
					return dateVal;
				}

				if (bool.TryParse(str, out var boolVal))
				{
					return boolVal;
				}

				if (TimeSpan.TryParse(str, culture, out var timeVal))
				{
					return timeVal;
				}
			}

			return base.GetValue(ordinal);
		}
	}
}
