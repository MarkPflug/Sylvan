using System.Data.Common;

namespace Sylvan.Data.Csv;

// SqlBulkCopy won't convert empty strings, but requires that the string be null
// when inserting into non-string columns. I've made the philosophical decision that
// by default the CsvDataReader schema is non-null strings, so a missing field is
// returned as an empty string. This adapter can be used to turn empty strings into
// nulls to make SqlBulkCopy happy.
class EmptyAsNullDataReader : DataReaderAdapter
{
	public EmptyAsNullDataReader(DbDataReader dr) : base(dr)
	{
	}

	public override string GetString(int ordinal)
	{
		var str = base.GetString(ordinal);
		return str?.Length == 0 ? null : str;
	}

	public override object GetValue(int ordinal)
	{
		return this.GetString(ordinal);
	}
}
