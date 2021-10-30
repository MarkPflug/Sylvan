using BenchmarkDotNet.Attributes;
using Sylvan.Data;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Sylvan.Benchmarks
{
	[MemoryDiagnoser]
	public class DataBinderBenchmarks
	{
		class TestRecord : DbDataReader
		{
			string[] columns;
			Type[] types;
			Dictionary<string, int> ordinals;

			public TestRecord()
			{
				this.columns = new[] { "B", "D", "V", "G", "I", "S" };
				this.types = new[] { typeof(bool), typeof(DateTime), typeof(double), typeof(Guid), typeof(int), typeof(string) };
				this.ordinals = columns.Select((n, i) => new { Name = n, Index = i }).ToDictionary(p => p.Name, p => p.Index);

			}

			public override object this[int i] => GetValue(i);

			public override object this[string name] => ordinals[name];

			public override int FieldCount => columns.Length;

			public override int Depth => 1;

			public override bool IsClosed => false;

			public override int RecordsAffected => throw new NotImplementedException();

			public override bool HasRows => true;

			public override bool GetBoolean(int i)
			{
				if (i == 0) return true;
				throw new NotImplementedException();
			}

			public override byte GetByte(int i)
			{
				throw new NotImplementedException();
			}

			public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
			{
				throw new NotImplementedException();
			}

			public override char GetChar(int i)
			{
				throw new NotImplementedException();
			}

			public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
			{
				throw new NotImplementedException();
			}

			public override string GetDataTypeName(int i)
			{
				return types[i].Name;
			}

			DateTime date = DateTime.UtcNow;
			public override DateTime GetDateTime(int i)
			{
				if (i == 1) return date;
				throw new NotSupportedException();
			}

			public override decimal GetDecimal(int i)
			{
				throw new NotSupportedException();
			}

			public override double GetDouble(int i)
			{
				if (i == 2) return 12345.5;
				throw new NotSupportedException();
			}

			public override Type GetFieldType(int i)
			{
				return types[i];
			}

			public override float GetFloat(int i)
			{
				throw new NotImplementedException();
			}

			Guid g = Guid.NewGuid();
			public override Guid GetGuid(int i)
			{
				if (i == 3) return g;
				throw new NotSupportedException();
			}

			public override short GetInt16(int i)
			{
				throw new NotImplementedException();
			}

			public override int GetInt32(int i)
			{
				if (i == 4) return 64532;
				throw new NotSupportedException();
			}

			public override long GetInt64(int i)
			{
				throw new NotImplementedException();
			}

			public override string GetName(int i)
			{
				return columns[i];
			}

			public override int GetOrdinal(string name)
			{
				return ordinals[name];
			}

			public override string GetString(int i)
			{
				if (i == 5) return "This is a test string";
				throw new NotSupportedException();
			}

			public override object GetValue(int i)
			{
				switch (i)
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

			public override int GetValues(object[] values)
			{
				throw new NotImplementedException();
			}

			public override bool IsDBNull(int i)
			{
				return false;
			}

			public override void Close()
			{
			}

			public override DataTable GetSchemaTable()
			{
				throw new NotImplementedException();
			}

			public override bool NextResult()
			{
				return false;
			}

			public override bool Read()
			{
				return true;
			}

			public override IEnumerator GetEnumerator()
			{
				throw new NotImplementedException();
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
			//this.reflection = new ReflectionDataBinder<Record>(schema.GetColumnSchema());
		}

		class ManualBinder : IDataBinder<Record>
		{
			public void Bind(DbDataReader record, Record item)
			{
				item.B = record.GetBoolean(0);
				item.D = record.GetDateTime(1);
				item.V = record.GetDouble(2);
				item.G = record.GetGuid(3);
				item.I = record.GetInt32(4);
				item.S = record.GetString(5);
			}

			public void Bind(DbDataReader record, object item)
			{
				Bind(record, (Record)item);
			}			
		}

		Record item;
		DbDataReader record;
		IDataBinder<Record> compiled;//, reflection;

		//[Benchmark]
		//public void Reflection()
		//{
		//	Bench(reflection, record);
		//}

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

		static void Bench(IDataBinder<Record> binder, DbDataReader record, Record item)
		{
			for (int i = 0; i < Count; i++)
			{
				binder.Bind(record, item);
			}
		}

		static void Bench(IDataBinder<Record> binder, DbDataReader record)
		{
			for (int i = 0; i < Count; i++)
			{
				var item = new Record();
				binder.Bind(record, item);
			}
		}
	}
}
