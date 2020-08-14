using BenchmarkDotNet.Attributes;
using Sylvan.Data;
using System;
using System.Collections.Generic;
using System.Data;

namespace Sylvan.Benchmarks
{
	[MemoryDiagnoser]
	public class DataBinderBenchmarks
	{
		class TestRecord : IDataRecord
		{
			public object this[int i] => throw new NotImplementedException();

			public object this[string name] => throw new NotImplementedException();

			public int FieldCount => throw new NotImplementedException();

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
				throw new NotImplementedException();
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
				throw new NotImplementedException();
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
				throw new NotImplementedException();
			}

			Dictionary<string, int> ordinals = new Dictionary<string, int>()
			{
				{ "B", 0 },
				{ "D", 1 },
				{ "V", 2 },
				{ "G", 3 },
				{ "I", 4 },
				{ "S", 5 },
			};

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
		}

		const int Count = 100000;

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
			var schema = Schema.TryParse("B:Boolean,D:DateTime,V:Double,G:Guid,I:Int32,S:String");
			this.record = new TestRecord();
			this.item = new Record();
			this.compiled = new CompiledDataBinder<Record>(schema.GetColumnSchema());
			this.reflection = new ReflectionDataBinder<Record>(schema.GetColumnSchema());
			this.direct = new ReflectionDirectDataBinder<Record>(schema.GetColumnSchema());
		}

		Record item;
		IDataRecord record;
		DataBinder<Record> compiled, reflection, direct;

		[Benchmark(Baseline = true)]
		public void Reflection()
		{
			Bench(reflection, record, item);
		}

		[Benchmark]
		public void DirectReflection()
		{
			Bench(direct, record, item);
		}

		[Benchmark]
		public void Compiled()
		{
			Bench(compiled, record, item);
		}

		static void Bench(DataBinder<Record> binder, IDataRecord record, Record item)
		{
			for (int i = 0; i < Count; i++)
			{
				binder.Bind(record, item);
			}
		}
	}
}
