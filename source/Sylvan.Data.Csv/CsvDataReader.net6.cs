#if NET6_0_OR_GREATER

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv;

partial class CsvDataAccessor :
	IFieldAccessor<DateOnly>,
	IFieldAccessor<TimeOnly>
{
	DateOnly IFieldAccessor<DateOnly>.GetValue(CsvDataReader reader, int ordinal)
	{
		return reader.GetDate(ordinal);
	}

	TimeOnly IFieldAccessor<TimeOnly>.GetValue(CsvDataReader reader, int ordinal)
	{
		return reader.GetTime(ordinal);
	}
}

partial class CsvDataReader
{
	readonly string? dateOnlyFormat;

	/// <summary>
	/// Gets the value of the field as a <see cref="DateOnly"/>.
	/// </summary>
	public DateOnly GetDate(int ordinal)
	{
		var span = this.GetFieldSpan(ordinal);
		if (IsoDate.TryParse(span, out DateOnly value))
		{
			return value;
		}

		var format = columns[ordinal].Format ?? dateOnlyFormat;
		var style = DateTimeStyles.None;
		if (format != null && DateOnly.TryParseExact(span, format, culture, style, out value))
		{
			return value;
		}
		return DateOnly.Parse(span, culture, style);
	}

	/// <summary>
	/// Gets the value of the field as a <see cref="TimeOnly"/>.
	/// </summary>
	public TimeOnly GetTime(int ordinal)
	{
		var format = columns[ordinal].Format;
		var span = this.GetFieldSpan(ordinal);
		var style = DateTimeStyles.None;
		if (format != null && TimeOnly.TryParseExact(span, format, culture, style, out var value))
		{
			return value;
		}
		return TimeOnly.Parse(span, culture, style);
	}

	/// <inheritdoc/>
	public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
	{
		return Task.FromResult(GetFieldValue<T>(ordinal));
	}
}

#endif
