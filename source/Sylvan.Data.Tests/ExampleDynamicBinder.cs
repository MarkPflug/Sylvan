using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data
{
	public class ExampleClass
	{
		public string Name { get; set; }
		public int Count { get; set; }
		public double Value { get; set; }
	}

	public static class Ext
	{
		public static IEnumerable<ExampleClass> Bind(this IDataReader reader) {
			var r = (DbDataReader)reader;
			var binder = new ExampleDynamicBinder(r.GetColumnSchema());
			while (reader.Read())
			{
				var item = new ExampleClass();
				binder.Bind(reader, item);
				yield return item;
			}
		}
	}

	sealed class ExampleDynamicBinder : IDataBinder<ExampleClass>
	{
		// TODO: maybe call this "MissingAccessor<T>"?
		static class NullAccessor<T>
		{
			public static Func<IDataRecord, int, T> Instance;

			static NullAccessor()
			{
				Instance = (dr, i) => default;
			}
		}

		// these should be inlined.
		int idx0, idx1, idx2;
		object accessor0, accessor1, accessor2;

		public ExampleDynamicBinder(IReadOnlyList<DbColumn> schema)
		{
			// generated
			idx0 = -1;
			idx1 = -1;
			idx2 = -1;

			accessor0 = NullAccessor<string>.Instance;
			accessor1 = NullAccessor<int>.Instance;
			accessor2 = NullAccessor<double>.Instance;

			int i = 0;

			foreach (var col in schema)
			{
				switch (col.ColumnName)
				{
					case "Name":
						idx0 = col.ColumnOrdinal ?? i;
						accessor0 = GetAccessor<string>(col);
						break;
					case "Count":
						idx1 = col.ColumnOrdinal ?? i;
						accessor1 = GetAccessor<int>(col);
						break;
					case "Value":
						idx2 = col.ColumnOrdinal ?? i;
						accessor2 = GetAccessor<double>(col);
						break;
				}
				i++;
			}
		}

		static Func<IDataRecord, int, T> GetAccessor<T>(DbColumn col)
		{

			if (col.AllowDBNull == false)
			{
				return (IDataRecord record, int ordinal) =>
				{
					return ((DbDataReader)record).GetFieldValue<T>(ordinal);
				};
			}
			else
			{
				return (IDataRecord record, int ordinal) =>
				{
					return
						record.IsDBNull(ordinal)
						? default
						: ((DbDataReader)record).GetFieldValue<T>(ordinal);
				};
			}
		}

		public void Bind(IDataRecord record, ExampleClass item)
		{
			item.Name = ((Func<IDataRecord, int, string>)accessor0)(record, idx0);
			item.Count = ((Func<IDataRecord, int, int>)accessor1)(record, idx1);
			item.Value = ((Func<IDataRecord, int, double>)accessor2)(record, idx2);
		}
	}
}
