#if NET6_0_OR_GREATER

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
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
		/// <summary>
		/// Gets the value of the field as a <see cref="DateOnly"/>.
		/// </summary>
		public DateOnly GetDate(int ordinal)
		{
			var f = this.GetField(ordinal);
			return DateOnly.TryParse(f.ToSpan(), out var value) ? value : throw new FormatException();
		}

		/// <summary>
		/// Gets the value of the field as a <see cref="TimeOnly"/>.
		/// </summary>
		public TimeOnly GetTime(int ordinal)
		{
			var f = this.GetField(ordinal);
			return TimeOnly.TryParse(f.ToSpan(), out var value) ? value : throw new FormatException();
		}

		/// <inheritdoc/>
		public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
		{
			return Task.FromResult(GetFieldValue<T>(ordinal));
		}
	}
}

#endif
