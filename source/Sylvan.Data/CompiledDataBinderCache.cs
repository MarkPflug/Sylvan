using System.Collections.Concurrent;
using System.Data.Common;

namespace Sylvan.Data;

sealed class CompiledBinderCache<T>
	where T : class
{
	static ConcurrentDictionary<SchemaKey, CompiledDataBinder<T>> SchemaCache =
		new ConcurrentDictionary<SchemaKey, CompiledDataBinder<T>>();

	internal static CompiledDataBinder<T> GetBinder(DbDataReader reader, DataBinderOptions opts)
	{
		var s = ((IDbColumnSchemaGenerator)reader).GetColumnSchema();
		var key = new SchemaKey(s);
		return SchemaCache.GetOrAdd(key, (key) => new CompiledDataBinder<T>(opts, s));
	}
}
