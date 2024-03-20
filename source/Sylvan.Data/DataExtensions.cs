using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Sylvan.Data;

/// <summary>
/// Extension methods for DbDataReader.
/// </summary>
public static partial class DataExtensions
{
	/// <summary>
	/// Converts an IDataReader to a DbDataReader.
	/// This conversion might be a no-op if the IDataReader is already
	/// a DbDataReader, or it might adapt the reader using a wrapper.
	/// </summary>
	public static DbDataReader AsDbDataReader(this IDataReader reader)
	{
		if (reader is DbDataReader dr) return dr;
		return new IDataReaderAdapter(reader);
	}

	/// <summary>
	/// Creates a DataTable that specifies the schema.
	/// </summary>
	public static DataTable ToSchemaTable(this System.Collections.ObjectModel.ReadOnlyCollection<DbColumn> schema)
	{
		return SchemaTable.GetSchemaTable(schema);
	}

	/// <summary>
	/// Creates a DbDataReader by attaching additional columns to an existing DbDataReader.
	/// </summary>
	/// <param name="reader">The base data reader.</param>
	/// <param name="columns">The extra columns to attach.</param>
	/// <returns>A DbDataReader.</returns>
	public static DbDataReader WithColumns(this DbDataReader reader, params IDataColumn[] columns)
	{
		var cols = new IDataColumn[reader.FieldCount + columns.Length];
		int i = 0;

		var schema = reader.CanGetColumnSchema() ? reader.GetColumnSchema() : null;
		for (; i < reader.FieldCount; i++)
		{
			var allowNull = schema?[i].AllowDBNull ?? true;
			cols[i] = new DataReaderColumn(reader, i, allowNull);
		}
		Array.Copy(columns, 0, cols, i, columns.Length);

		return new MappedDataReader(reader, cols);
	}

	/// <summary>
	/// Binds the DbDataReader data to produce a sequence of T.
	/// </summary>
	/// <typeparam name="T">The type of record to bind to.</typeparam>
	/// <param name="reader">The data reader.</param>
	public static IEnumerable<T> GetRecords<T>(this DbDataReader reader)
		where T : class, new()
	{
		return GetRecords<T>(reader, null);
	}

	/// <summary>
	/// Binds the DbDataReader data to produce a sequence of T.
	/// </summary>
	/// <typeparam name="T">The type of record to bind to.</typeparam>
	/// <param name="reader">The data reader.</param>
	/// <param name="opts">The options to configure the data binder.</param>
	public static IEnumerable<T> GetRecords<T>(this DbDataReader reader, DataBinderOptions? opts = null)
		where T : class, new()
	{
		var binder = DataBinder.Create<T>(reader, opts);
		while (reader.Read())
		{
			var item = new T();
			binder.Bind(reader, item);
			yield return item;
		}
	}

	/// <summary>
	/// Creates a DbDataReader that reads until a certain condition is met.
	/// </summary>
	/// <param name="reader">The base DbDataReader</param>
	/// <param name="predicate">The predicate, which once false will end the reader.</param>
	/// <returns>A DbDataReader</returns>
	public static DbDataReader TakeWhile(this DbDataReader reader, Func<DbDataReader, bool> predicate)
	{
		if (predicate == null) throw new ArgumentNullException(nameof(predicate));
		return new TakeWhileDataReader(reader, predicate);
	}

#if IAsyncEnumerable

	/// <summary>
	/// Binds the DbDataReader data to produce a sequence of T.
	/// </summary>
	/// <typeparam name="T">The type of record to bind to.</typeparam>
	/// <param name="reader">The data reader.</param>
	public static IAsyncEnumerable<T> GetRecordsAsync<T>(this DbDataReader reader)
		where T : class, new()
	{
		return GetRecordsAsync<T>(reader, null, default);
	}

	/// <summary>
	/// Binds the DbDataReader data to produce a sequence of T.
	/// </summary>
	/// <typeparam name="T">The type of record to bind to.</typeparam>
	/// <param name="reader">The data reader.</param>
	/// <param name="opts">The options to configure the data binder.</param>
	public static IAsyncEnumerable<T> GetRecordsAsync<T>(this DbDataReader reader, DataBinderOptions? opts)
		where T : class, new()
	{
		return GetRecordsAsync<T>(reader, opts, default);
	}

	/// <summary>
	/// Binds the DbDataReader data to produce a sequence of T.
	/// </summary>
	/// <typeparam name="T">The type of record to bind to.</typeparam>
	/// <param name="reader">The data reader.</param>
	/// <param name="opts">The options to configure the data binder.</param>
	/// <param name="cancel">The cancellation token</param>
	public static async IAsyncEnumerable<T> GetRecordsAsync<T>(this DbDataReader reader, DataBinderOptions? opts = null, [EnumeratorCancellation] CancellationToken cancel = default)
		where T : class, new()
	{
		var binder = DataBinder.Create<T>(reader, opts);
		while (await reader.ReadAsync(cancel).ConfigureAwait(false))
		{
			var item = new T();
			binder.Bind(reader, item);
			yield return item;
		}
	}

	/// <example>
	/// var reader = seq.AsDataReader()
	/// </example>
	public static DbDataReader AsDataReader<T>(this IAsyncEnumerable<T> seq, CancellationToken cancel = default)
		where T : class
	{
		return new AsyncObjectDataReader<T>(seq, cancel);
	}

#endif

	/// <example>
	/// var reader = seq.AsDataReader()
	/// </example>
	public static DbDataReader AsDataReader<T>(this IEnumerable<T> seq)
		where T : class
	{
		return new SyncObjectDataReader<T>(seq);
	}

	/// <summary>
	/// Selects a subset of columns for a DbDataReader.
	/// </summary>
	/// <param name="reader">The DbDataReader to select columns from.</param>
	/// <param name="ordinalsSelector">A function to select the column ordinals.</param>
	/// <returns>Returns a new DbDataReader containing just the selected columns.</returns>
	public static DbDataReader Select(this DbDataReader reader, Func<DbDataReader, int[]> ordinalsSelector)
	{
		var o = ordinalsSelector(reader);
		return new TransformDataReader(reader, o);
	}

	/// <summary>
	/// Selects a subset of columns for a DbDataReader.
	/// </summary>
	/// <param name="reader">The DbDataReader to select columns from.</param>
	/// <param name="ordinals">A column ordinals to select.</param>
	/// <returns>Returns a new DbDataReader containing just the selected columns.</returns>
	public static DbDataReader Select(this DbDataReader reader, params int[] ordinals)
	{
		return new TransformDataReader(reader, ordinals);
	}

	/// <summary>
	/// Selects a subset of columns for a DbDataReader.
	/// </summary>
	/// <param name="reader">The DbDataReader to select columns from.</param>
	/// <param name="columnNames">The names of the columns to select.</param>
	/// <returns>Returns a new DbDataReader containing just the selected columns.</returns>
	public static DbDataReader Select(this DbDataReader reader, params string[] columnNames)
	{
		var ordinals = GetOrdinals(reader, columnNames);
		return new TransformDataReader(reader, ordinals);
	}


	/// <summary>
	/// Selects a subset of columns for a DbDataReader.
	/// </summary>
	/// <param name="reader">The DbDataReader to select columns from.</param>
	/// <param name="columns">The column projections.</param>
	/// <returns>Returns a new DbDataReader containing just the selected columns.</returns>
	public static DbDataReader Select(this DbDataReader reader, params IDataColumn[] columns)
	{
		return new MappedDataReader(reader, columns);
	}

	static int[] GetOrdinals(DbDataReader reader, string[] names)
	{
		int[] ordinals = new int[names.Length];

		for (int i = 0; i < names.Length; i++)
		{
			var idx = reader.GetOrdinal(names[i]);
			if (idx < 0)
				throw new ArgumentOutOfRangeException(nameof(names));
			ordinals[i] = idx;
		}
		return ordinals;
	}

	/// <summary>
	/// Applies a filter predicate to the rows of a DbDataReader.
	/// </summary>
	/// <param name="reader">A DbDataReader.</param>
	/// <param name="predicate">A filter predicate to filter the rows.</param>
	/// <returns>A new DbDataReader that produces the filtered rows.</returns>
	public static DbDataReader Where(this DbDataReader reader, Func<DbDataReader, bool> predicate)
	{
		if (reader == null) throw new ArgumentNullException(nameof(reader));
		if (predicate == null) throw new ArgumentNullException(nameof(predicate));
		// TODO: TransformDataReader needs to merge into a new object rather than
		// nest. nesting will lead to excessive method call overhead.
		return new TransformDataReader(reader, null, predicate);
	}

	/// <summary>
	/// Creates a DbDataReader that reads the first number of rows.
	/// </summary>
	/// <param name="reader">The base data reader.</param>
	/// <param name="count">The maximum number of rows to read.</param>
	/// <returns>A DbDataReader.</returns>
	public static DbDataReader Take(this DbDataReader reader, int count)
	{
		if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
		return new SkipTakeDataReader(reader, -1, count);
	}

	/// <summary>
	/// Creates a DbDataReader that skips the first number of rows.
	/// </summary>
	/// <param name="reader">The base data reader.</param>
	/// <param name="count">The number of rows to skip.</param>
	/// <returns>A DbDataReader.</returns>
	public static DbDataReader Skip(this DbDataReader reader, int count)
	{
		if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
		return new SkipTakeDataReader(reader, count, -1);
	}

	/// <summary>
	/// Creates a DbDataReader where FieldCount can return different values for each row.
	/// </summary>
	/// <remarks>
	/// Most DbDataReader implementations work on purely rectangular data. However, some implementations
	/// might operate file formats that support variable fields. This allows accessing those extra columns
	/// using the standard DbDataReader base type APIs only. 
	/// Specifically, this is to support the Sylvan CSV and Excel data readers.
	/// </remarks>
	/// <param name="reader">A DbDataReader implementation.</param>
	/// <param name="rowFieldCountAccessor">A function that returns the number of </param>
	/// <param name="fieldType">Gets the type of the extra fields</param>
	public static DbDataReader AsVariableField<T>(this T reader, Func<T, int> rowFieldCountAccessor, Type? fieldType = null)
		where T : DbDataReader
	{
		fieldType ??= typeof(object);
		return new VariableDataReader<T>(reader, rowFieldCountAccessor, fieldType);
	}

	/// <summary>
	/// Creates a DbDataReader that validates data against a schema as it reads.
	/// </summary>
	/// <param name="reader">A DbDataReader</param>
	/// <param name="validationHandler">A DataValidationHandler callback.</param>
	/// <returns>A DbDataReader.</returns>
	[Obsolete("This feature is preview and might change in future release.")]
	public static DbDataReader ValidateSchema(this DbDataReader reader, DataValidationHandler validationHandler)
	{
		return new ValidatingDataReader(reader, validationHandler);
	}

	/// <summary>
	/// Creates a DbDataReader that validates data against a schema as it reads.
	/// The validationHandler method will be called for every row.
	/// </summary>
	/// <param name="reader">A DbDataReader</param>
	/// <param name="validationHandler">A DataValidationHandler callback.</param>
	/// <returns>A DbDataReader.</returns>
	[Obsolete("This feature is preview and might change in future release.")]
	public static DbDataReader Validate(this DbDataReader reader, DataValidationHandler validationHandler)
	{
		return new ValidatingDataReader(reader, validationHandler, true);
	}
}
