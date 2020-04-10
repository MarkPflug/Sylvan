using System.Data.Common;

namespace Sylvan.Data.Csv
{
	public interface ICsvSchemaProvider
	{
		DbColumn? GetColumn(string? name, int ordinal);
	}
}
