using BenchmarkDotNet.Attributes;
using Sylvan.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Sylvan.Benchmarks
{
	[MemoryDiagnoser]
	public class DataBinderBenchmarks
	{
		class TestRecord : IDataReader
		{
			string[] columns;
			Type[] types;
			Dictionary<string, int> ordinals;

			public TestRecord()
			{
				this.columns = new[] { "B", "D", "V", "G", "I", "S"};
				this.types = new[] { typeof(bool), typeof(DateTime), typeof(double), typeof(Guid), typeof(int), typeof(string) };
				this.ordinals = columns.Select((n, i) => new { Name = n, Index = i }).ToDictionary(p => p.Name, p => p.Index);

			}

			public object this[int i] => GetValue(i);

			public object this[string name] => ordinals[name];

			public int FieldCount => columns.Length;

			public int Depth => 1;

			public bool IsClosed => false;

			public int RecordsAffected => throw new NotImplementedException();

			public bool GetBoolean(int i)
			{
				if (i == 0) return true;
				throw new NotImplementedException();
			}

			public byte GetByte(int i)
			{
				throw new NotImplementedException();
			}

			public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
			{
				throw new NotImplementedException();
			}

			public char GetChar(int i)
			{
				throw new NotImplementedException();
			}

			public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
			{
				throw new NotImplementedException();
			}

			public IDataReader GetData(int i)
			{
				throw new NotImplementedException();
			}

			public string GetDataTypeName(int i)
			{
				return types[i].Name;
			}

			DateTime date = DateTime.UtcNow;
			public DateTime GetDateTime(int i)
			{
				if (i == 1) return date;
				throw new NotSupportedException();
			}

			public decimal GetDecimal(int i)
			{
				throw new NotSupportedException();
			}

			public double GetDouble(int i)
			{
				if (i == 2) return 12345.5;
				throw new NotSupportedException();
			}

			public Type GetFieldType(int i)
			{
				return types[i];
			}

			public float GetFloat(int i)
			{
				throw new NotImplementedException();
			}

			Guid g = Guid.NewGuid();
			public Guid GetGuid(int i)
			{
				if (i == 3) return g;
				throw new NotSupportedException();
			}

			public short GetInt16(int i)
			{
				throw new NotImplementedException();
			}

			public int GetInt32(int i)
			{
				if (i == 4) return 64532;
				throw new NotSupportedException();
			}

			public long GetInt64(int i)
			{
				throw new NotImplementedException();
			}

			public string GetName(int i)
			{
				return columns[i];
			}

			public int GetOrdinal(string name)
			{
				return ordinals[name];
			}

			public string GetString(int i)
			{
				if (i == 5) return "This is a test string";
				throw new NotSupportedException();
			}

			public object GetValue(int i)
			{
				switch(i)
				{
					case 0: return GetBoolean(i);
					case 1: return GetDateTime(i);
					case 2: return GetDouble(i);
					case 3: return GetGuid(i);
					case 4: return GetInt32(i);
					case 5: return GetString(i);
				}
				throw new NotSupportedException();
			}

			public int GetValues(object[] values)
			{
				throw new NotImplementedException();
			}

			public bool IsDBNull(int i)
			{
				return false;
			}

			public void Close()
			{
			}

			public DataTable GetSchemaTable()
			{
				throw new NotImplementedException();
			}

			public bool NextResult()
			{
				return false;
			}

			public bool Read()
			{
				return true;
			}

			public void Dispose()
			{
			}
		}

		const int Count = 10000000;

		class Record
		{
			public bool B { get; set; }
			public DateTime D { get; set; }
			public double V { get; set; }
			public Guid G { get; set; }
			public int I { get; set; }
			public string S { get; set; }
		}

		public DataBinderBenchmarks()
		{
			var schema = Schema.Parse("B:Boolean,D:DateTime,V:Double,G:Guid,I:Int32,S:String");
			this.record = new TestRecord();
			this.item = new Record();
			this.compiled = new CompiledDataBinder<Record>(DataBinderOptions.Default, schema.GetColumnSchema());
			this.reflection = new ReflectionDataBinder<Record>(schema.GetColumnSchema());
		}

		class ManualBinder : IDataBinder<Record>
		{
			public void Bind(IDataRecord record, Record item)
			{
				item.B = record.GetBoolean(0);
				item.D = record.GetDateTime(1);
				item.V = record.GetDouble(2);
				item.G = record.GetGuid(3);
				item.I = record.GetInt32(4);
				item.S = record.GetString(5);
			}
		}

		Record item;
		IDataReader record;
		IDataBinder<Record> compiled, reflection;

		[Benchmark]
		public void Reflection()
		{
			Bench(reflection, record);
		}

		[Benchmark]
		public void Compiled()
		{
			Bench(compiled, record);
		}

		[Benchmark(Baseline = true)]
		public void Manual()
		{
			Bench(compiled, record);
		}

		static void Bench(IDataBinder<Record> binder, IDataRecord record, Record item)
		{
			for (int i = 0; i < Count; i++)
			{
				binder.Bind(record, item);
			}
		}

		static void Bench(IDataBinder<Record> binder, IDataRecord record)
		{
			for (int i = 0; i < Count; i++)
			{
				var item = new Record();
				binder.Bind(record, item);
			}
		}
	}
}
