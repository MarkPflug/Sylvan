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
		public static IEnumerable<ExampleClass> Bind(this DbDataReader reader)
		{
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

	//sealed class ExampleDynamicBinderFactory : IDataBinderFactory<ExampleClass>
	//{
	//	public IDataBinder<ExampleClass> Create(IReadOnlyList<DbColumn> schema, DataBinderOptions options)
	//	{
	//		return new ExampleDynamicBinder(schema);
	//	}
	//}

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

		static Dictionary<string, int> Map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
		{
			{"Name", 0 },
			{"Count", 1 },
			{"Value", 2 },
		};

		int idx0, idx1, idx2;
		Func<IDataRecord, int, string> accessor0;
		Func<IDataRecord, int, int> accessor1;
		Func<IDataRecord, int, double> accessor2;

		public static void F()
		{
			throw new NotImplementedException();
		}

		public ExampleDynamicBinder(IReadOnlyList<DbColumn> schema)
		{
			// this method body will be generated with ILEmit
			// it is called one time to construct a binder from DbDataReader to T.
			idx0 = -1;
			idx1 = -1;
			idx2 = -1;

			accessor0 = NullAccessor<string>.Instance;
			accessor1 = NullAccessor<int>.Instance;
			accessor2 = NullAccessor<double>.Instance;
			int i = 0;
			foreach (var col in schema)
			{
				if (Map.TryGetValue(col.ColumnName, out int ord))
				{

					switch (ord)
					{
						case 0:
							idx0 = i;
							accessor0 = GetAccessor<string>(col);
							break;
						case 1:
							idx1 = i;
							accessor1 = GetAccessor<int>(col);
							break;
						case 2:
							idx2 = i;
							accessor2 = GetAccessor<double>(col);
							break;
					}
					i++;
				}
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

		public void Bind(DbDataReader record, ExampleClass item)
		{
			// this method body will be generated with ILEmit
			// called once per row in a DbDataReader.
			item.Name = accessor0(record, idx0);
			item.Count = accessor1(record, idx1);
			item.Value = accessor2(record, idx2);
		}

		public void Bind(DbDataReader record, object item)
		{
			Bind(record, (ExampleClass)item);
		}
	}
}
