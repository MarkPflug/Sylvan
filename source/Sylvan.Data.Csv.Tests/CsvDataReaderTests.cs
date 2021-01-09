using Sylvan.Benchmarks;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class CsvDataReaderTests
	{
		[Fact]
		public void SylvanBench()
		{
			var b = new CsvDataBinderBenchmarks();
			b.SylvanBench();
		}

		[Fact]
		public async Task SylvanSchemaBench()
		{
			var b = new CsvReaderBenchmarks();
			await b.SylvanSchema();
		}

		[Fact]
		public void FinalCRTest()
		{
			var b = CsvDataReader.Create(new StringReader(@"A,B\r\n1,2\r\n1,\r"));
			while (b.Read()) ;
		}

		[Fact]
		public void MissingFinalFieldTest()
		{
			var b = CsvDataReader.Create(new StringReader(@"A,B\r\n1,2\r\n1,"));
			while (b.Read()) ;
		}

		[Fact]
		public void MissingFinalField2Test()
		{
			var b = CsvDataReader.Create(new StringReader(@"A,B\r\n1"));
			while (b.Read()) ;
		}

		[Fact]
		public async Task Simple()
		{
			using var csv = await CsvDataReader.CreateAsync("Data\\Simple.csv");
			Assert.Equal(4, csv.FieldCount);
			Assert.True(csv.HasRows);
			Assert.Equal(0, csv.RowNumber);
			Assert.Equal("Id", csv.GetName(0));
			Assert.Equal("Name", csv.GetName(1));
			Assert.Equal("Value", csv.GetName(2));
			Assert.Equal("Date", csv.GetName(3));
			Assert.True(await csv.ReadAsync());
			Assert.Equal(1, csv.RowNumber);
			Assert.Equal("1", csv[0]);
			Assert.Equal("John", csv[1]);
			Assert.Equal("Low", csv[2]);
			Assert.Equal("2000-11-11", csv[3]);
			Assert.True(await csv.ReadAsync());
			Assert.Equal(2, csv.RowNumber);
			Assert.Equal("2", csv[0]);
			Assert.Equal("Jane", csv[1]);
			Assert.Equal("High", csv[2]);
			Assert.Equal("1989-03-14", csv[3]);
			Assert.False(await csv.ReadAsync());
		}

		const string BinaryValue1 = "Hello, world!";
		const string BinaryValue2 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";

		[Fact]
		public async Task Binary()
		{
			using (var reader = File.OpenText("Data\\Binary.csv"))
			{
				var csv = await CsvDataReader.CreateAsync(reader);
				csv.Read();
				var id = csv.GetInt32(0);
				byte[] buffer = new byte[16];
				var len = csv.GetBytes(1, 0, buffer, 0, 2);
				Assert.Equal(2, len);
				len += csv.GetBytes(1, len, buffer, (int)len, 14);
				Assert.Equal(BinaryValue1.Length, len);
				var expected = Encoding.ASCII.GetBytes(BinaryValue1);
				Assert.Equal(expected, buffer.Take((int)len));
				csv.Read();

				var p = 0;
				len = 0;
				var sw = new StringWriter();
				while ((len = csv.GetBytes(1, p, buffer, 0, buffer.Length)) != 0)
				{
					p += (int)len;
					sw.Write(Encoding.ASCII.GetString(buffer, 0, (int)len));
				}
				var str = sw.ToString();
				Assert.Equal(BinaryValue2, str);
			}
		}

		[Fact]
		public async Task BinaryValues()
		{
			using (var reader = File.OpenText("Data\\Binary.csv"))
			{
				var schema = new TypedCsvSchema();
				schema.Add(1, typeof(byte[]));
				var opts = new CsvDataReaderOptions() { Schema = schema };
				var csv = await CsvDataReader.CreateAsync(reader, opts);
				csv.Read();
				var vals = new object[2];
				csv.GetValues(vals);
				var expected = Encoding.ASCII.GetBytes(BinaryValue1);
				Assert.Equal(expected, (byte[])vals[1]);
				csv.Read();
				csv.GetValues(vals);
				expected = Encoding.ASCII.GetBytes(BinaryValue2);
				Assert.Equal(expected, (byte[])vals[1]);
			}
		}

		[Fact]
		public async Task PartialRecord()
		{
			using (var reader = File.OpenText("Data\\PartialRecord.csv"))
			{
				var csv = await CsvDataReader.CreateAsync(reader);
				Assert.Equal(4, csv.FieldCount);
				Assert.True(csv.HasRows);
				Assert.Equal(0, csv.RowNumber);
				Assert.True(await csv.ReadAsync());
				Assert.Equal(1, csv.RowNumber);
				Assert.Equal("1", csv[0]);
				Assert.Equal("John", csv[1]);
				Assert.False(csv.IsDBNull(2));
				Assert.Equal("Low", csv[2]);
				Assert.False(csv.IsDBNull(3));
				Assert.Equal("", csv[3]);
			}
		}

		[Fact]
		public async Task Quoted()
		{
			using (var reader = File.OpenText("Data\\Quote.csv"))
			{
				var csv = await CsvDataReader.CreateAsync(reader);
				Assert.Equal(5, csv.FieldCount);
				Assert.True(csv.HasRows);
				Assert.Equal(0, csv.RowNumber);
				Assert.Equal("Id", csv.GetName(0));
				Assert.Equal("Name", csv.GetName(1));
				Assert.Equal("Value", csv.GetName(2));
				Assert.Equal("Date", csv.GetName(3));
				Assert.Equal("Original, Origin", csv.GetName(4));
				Assert.True(await csv.ReadAsync());
				Assert.Equal(1, csv.RowNumber);
				Assert.Equal("1", csv[0]);
				Assert.Equal("John", csv[1]);
				Assert.Equal("Very\r\nLow\r\n", csv[2]);
				Assert.Equal("2000-11-11", csv[3]);
				Assert.True(await csv.ReadAsync());
				Assert.Equal(2, csv.RowNumber);
				Assert.Equal("2", csv[0]);
				Assert.Equal("Jane", csv[1]);
				Assert.Equal("\"High\"", csv[2]);
				Assert.Equal("1989-03-14", csv[3]);
				Assert.True(await csv.ReadAsync());
				Assert.Equal(3, csv.RowNumber);
				Assert.Equal("3", csv[0]);
				Assert.Equal("Comma", csv[1]);
				Assert.Equal("Quite, Common", csv[2]);
				Assert.Equal("2020-05-29", csv[3]);
				Assert.Equal("", csv[4]);
				Assert.False(await csv.ReadAsync());
			}
		}

		[Fact]
		public void Quoted2()
		{
			var reader = new StringReader("a,b,c\n1,\"\"\"2\"\", (two)\",3");
			var csv = CsvDataReader.Create(reader);
			Assert.Equal(3, csv.FieldCount);
			csv.Read();
			Assert.Equal("1", csv.GetString(0));
			Assert.Equal("\"2\", (two)", csv.GetString(1));
			Assert.Equal("3", csv.GetString(2));
		}

		[Fact]
		public void MissingHeaders()
		{
			Assert.Throws<CsvMissingHeadersException>(() => CsvDataReader.Create(new StringReader("")));
		}

		[Fact]
		public async Task NoHeaders()
		{
			using (var reader = File.OpenText("Data\\DataOnly.csv"))
			{
				var csv = await CsvDataReader.CreateAsync(reader, new CsvDataReaderOptions { HasHeaders = false });
				Assert.Equal(4, csv.FieldCount);
				Assert.True(csv.HasRows);
				Assert.Equal(0, csv.RowNumber);
				Assert.Equal("", csv.GetName(0));
				Assert.Throws<IndexOutOfRangeException>(() => csv.GetOrdinal("Id"));
				Assert.True(await csv.ReadAsync());
				Assert.Equal(1, csv.RowNumber);
				Assert.Equal("1", csv[0]);
				Assert.Equal("John", csv[1]);
				Assert.Equal("Low", csv[2]);
				Assert.Equal("2000-11-11", csv[3]);
				Assert.True(await csv.ReadAsync());
				Assert.Equal(2, csv.RowNumber);
				Assert.Equal("2", csv[0]);
				Assert.Equal("Jane", csv[1]);
				Assert.Equal("High", csv[2]);
				Assert.Equal("1989-03-14", csv[3]);
				Assert.False(await csv.ReadAsync());
			}
		}

		[Fact]
		public async Task NoHeadersWithSchema()
		{
			var schema = new ExcelHeaders();
			var opts =
				new CsvDataReaderOptions
				{
					HasHeaders = false,
					Schema = schema
				};

			using (var reader = File.OpenText("Data\\DataOnly.csv"))
			{
				var csv = await CsvDataReader.CreateAsync(reader, opts);
				Assert.Equal(4, csv.FieldCount);
				Assert.True(csv.HasRows);
				Assert.Equal(0, csv.RowNumber);
				Assert.Equal("C", csv.GetName(2));
				Assert.Equal(3, csv.GetOrdinal("D"));
				Assert.Equal("A", csv.GetName(0));
				Assert.Throws<IndexOutOfRangeException>(() => csv.GetOrdinal("Id"));
				Assert.True(await csv.ReadAsync());
				Assert.Equal(1, csv.RowNumber);
				Assert.Equal("1", csv[0]);
				Assert.Equal("John", csv[1]);
				Assert.Equal("Low", csv[2]);
				Assert.Equal("2000-11-11", csv[3]);
				Assert.True(await csv.ReadAsync());
				Assert.Equal(2, csv.RowNumber);
				Assert.Equal("2", csv[0]);
				Assert.Equal("Jane", csv[1]);
				Assert.Equal("High", csv[2]);
				Assert.Equal("1989-03-14", csv[3]);
				Assert.False(await csv.ReadAsync());
			}
		}


		[Theory]
		[InlineData("Id,Name,Value,Date")]
		[InlineData("Id,Name,Value,Date\n")]
		[InlineData("Id,Name,Value,Date\r\n")]
		public async Task HeadersOnly(string data)
		{
			using (var reader = new StringReader(data))
			{
				var csv = await CsvDataReader.CreateAsync(reader);
				Assert.Equal(4, csv.FieldCount);
				Assert.False(csv.HasRows);
				Assert.Equal(0, csv.RowNumber);
				Assert.Equal("Id", csv.GetName(0));
				Assert.Equal("Name", csv.GetName(1));
				Assert.Equal("Value", csv.GetName(2));
				Assert.Equal("Date", csv.GetName(3));
				Assert.False(await csv.ReadAsync());
			}
		}

		[Fact]
		public async Task TestBig()
		{
			var tr = TestData.GetTextReader();
			string str = null;

			var csv = await CsvDataReader.CreateAsync(tr);
			while (await csv.ReadAsync())
			{
				var s = csv.GetString(0);
				var v = csv.GetInt32(0);
				var n = csv.GetString(10);

				for (int i = 0; i < csv.FieldCount; i++)
				{
					str = csv.GetString(i);
				}
				var v1 = csv.GetInt32(20);
			}
			// there was previously a bug, where the last value was read as "103\n".
			Assert.Equal("103", str);
		}

		[Fact]
		public void Sync()
		{
			using (var reader = File.OpenText("Data\\Simple.csv"))
			{
				var csv = CsvDataReader.Create(reader);
				Assert.Equal(4, csv.FieldCount);
				Assert.True(csv.HasRows);
				Assert.Equal(0, csv.RowNumber);
				Assert.Equal("Id", csv.GetName(0));
				Assert.Equal("Name", csv.GetName(1));
				Assert.Equal("Value", csv.GetName(2));
				Assert.Equal("Date", csv.GetName(3));
				Assert.True(csv.Read());
				Assert.Equal(1, csv.RowNumber);
				Assert.Equal("1", csv[0]);
				Assert.Equal("John", csv[1]);
				Assert.Equal("Low", csv[2]);
				Assert.Equal("2000-11-11", csv[3]);
				Assert.True(csv.Read());
				Assert.Equal(2, csv.RowNumber);
				Assert.Equal("2", csv[0]);
				Assert.Equal("Jane", csv[1]);
				Assert.Equal("High", csv[2]);
				Assert.Equal("1989-03-14", csv[3]);
				Assert.False(csv.Read());
			}
		}

		[Fact]
		public void Broken()
		{
			using (var reader = File.OpenText("Data\\Broken.csv"))
			{
				var csv = CsvDataReader.Create(reader);
				Assert.Equal(2, csv.FieldCount);
				Assert.True(csv.HasRows);
				Assert.Equal(0, csv.RowNumber);
				Assert.Equal("A", csv.GetName(0));
				Assert.Equal("B", csv.GetName(1));
				Assert.True(csv.Read());
				Assert.Equal(1, csv.RowNumber);
				Assert.Equal("ab", csv[0]);
				Assert.Equal("c", csv[1]);
				Assert.True(csv.Read());
				Assert.Equal(2, csv.RowNumber);
				Assert.Equal("d\"e\"f", csv[0]);
				Assert.Equal("gh\"i", csv[1]);
				Assert.False(csv.Read());
			}
		}

		[Fact]
		public void CustomSchema()
		{
			using var sr = TestData.GetTextReader();
			var schema = new TypedCsvSchema();
			schema.Add("UID", typeof(int));
			var csv = CsvDataReader.Create(sr, new CsvDataReaderOptions { Schema = schema });
		}

		[Fact]
		public void GetColumnSchema()
		{
			using (var reader = File.OpenText("Data\\Simple.csv"))
			{
				var csv = CsvDataReader.Create(reader);
				var cols = csv.GetColumnSchema();

				var names = new[] { "Id", "Name", "Value", "Date" };
				for (int i = 0; i < cols.Count; i++)
				{
					Assert.Equal(names[i], cols[i].ColumnName);
					Assert.Equal(typeof(string), cols[i].DataType);
				}
			}
		}

		class ExcelHeaders : ICsvSchemaProvider
		{
			public DbColumn GetColumn(string name, int ordinal)
			{
				return new ExcelColumn("" + (char)('A' + ordinal));
			}

			class ExcelColumn : DbColumn
			{
				public ExcelColumn(string name)
				{
					this.ColumnName = name;
				}
			}
		}

		[Fact]
		public void DupeHeader()
		{
			var data = "a,b,b";

			var csv = CsvDataReader.Create(new StringReader(data));
			Assert.Equal(0, csv.GetOrdinal("a"));
			Assert.Throws<AmbiguousColumnException>(() => csv.GetOrdinal("b"));
			var schema = csv.GetColumnSchema();
			Assert.Equal("a", schema[0].ColumnName);
			Assert.Equal("b", schema[1].ColumnName);
			Assert.Equal("b", schema[2].ColumnName);
		}

		[Fact]
		public void DupeHeaderFix()
		{
			var data = "a,b,c,d,e,e";

			var fixSchema = new ExcelHeaders();
			var opts = new CsvDataReaderOptions { Schema = fixSchema };

			var csv = CsvDataReader.Create(new StringReader(data), opts);
			Assert.Equal(6, csv.FieldCount);

		}

		class FixHeaders : ICsvSchemaProvider
		{

			HashSet<string> h;

			public FixHeaders()
			{
				this.h = new HashSet<string>();
			}

			class Column : DbColumn
			{
				public Column(string orignalName, string name)
				{
					this.BaseColumnName = orignalName;
					this.ColumnName = name;
				}
			}

			public DbColumn GetColumn(string name, int ordinal)
			{
				if (h.Add(name))
				{
					return new Column(name, name);
				}
				for (int i = 2; i < 100; i++)
				{
					var dedupe = name + i;
					if (h.Add(dedupe))
					{
						return new Column(name, dedupe);
					}
				}

				//exceptions are better than infinite loops.
				throw new NotSupportedException();
			}
		}

		[Fact]
		public void DupeHeaderFix2()
		{
			var data = "a,a,b,b,c,c";

			var fixSchema = new FixHeaders();
			var opts = new CsvDataReaderOptions { Schema = fixSchema };

			var csv = CsvDataReader.Create(new StringReader(data), opts);
			Assert.Equal(6, csv.FieldCount);

		}

		[Fact]
		public void GetSchemaTable()
		{
			using (var reader = File.OpenText("Data\\Simple.csv"))
			{
				var csv = CsvDataReader.Create(reader);
				var schema = csv.GetSchemaTable();

				var names = new[] { "Id", "Name", "Value", "Date" };
				for (int i = 0; i < schema.Rows.Count; i++)
				{
					var row = schema.Rows[i];
					Assert.Equal(names[i], (string)row["ColumnName"]);
					Assert.Equal(typeof(string), (Type)row["DataType"]);
				}
			}
		}

		[Fact]
		public void Enumerator()
		{
			using (var reader = File.OpenText("Data\\Simple.csv"))
			{
				var csv = CsvDataReader.Create(reader);
				int c = 0;
				foreach (IDataRecord row in csv)
				{
					c++;
					Assert.Same(row, csv);
				}
				Assert.Equal(2, c);
			}
		}

		[Fact]
		public void Create()
		{
			Assert.ThrowsAsync<ArgumentNullException>(() => CsvDataReader.CreateAsync((TextReader)null));
		}

		[Fact]
		public void BufferTooSmall()
		{
			var opts = new CsvDataReaderOptions() { BufferSize = 128 };
			using var tr = File.OpenText("Data\\Binary.csv");
			var csv = CsvDataReader.Create(tr, opts);
			csv.Read();
			Assert.Throws<CsvRecordTooLargeException>(() => csv.Read());
		}

		[Fact]
		public void NextResult()
		{
			using var tr = File.OpenText("Data\\Binary.csv");
			var csv = CsvDataReader.Create(tr);
			Assert.False(csv.NextResult());
			Assert.False(csv.Read());
		}

		[Fact]
		public async Task NextResultAsync()
		{
			using var tr = File.OpenText("Data\\Binary.csv");
			var csv = CsvDataReader.Create(tr);
			Assert.False(await csv.NextResultAsync());
			Assert.False(await csv.ReadAsync());
		}

		CsvDataReader GetTypedReader()
		{
			var tr = File.OpenText("Data\\Types.csv");
			var schema = new TypedCsvSchema();
			schema.Add("Byte", typeof(byte));
			schema.Add("Int16", typeof(short));
			schema.Add("Int32", typeof(int));
			schema.Add("Int64", typeof(long));
			schema.Add("Char", typeof(char));
			schema.Add("String", typeof(string));
			schema.Add("Bool", typeof(bool));
			schema.Add("Float", typeof(float));
			schema.Add("Double", typeof(double));
			schema.Add("DateTime", typeof(DateTime));
			schema.Add("Decimal", typeof(decimal));
			schema.Add("Guid", typeof(Guid));

			var opts = new CsvDataReaderOptions() { Schema = schema };

			return CsvDataReader.Create(tr, opts);
		}

		[Fact]
		public void Types()
		{
			var csv = GetTypedReader();
			csv.Read();
			var rowData = new object[csv.FieldCount];
			var count = csv.GetValues(rowData);
			var types = new HashSet<string>();
			for (var i = 0; i < csv.FieldCount; i++)
			{
				Assert.True(types.Add(csv.GetDataTypeName(i)));
			}
			Assert.Equal(rowData.Length, count);
			foreach (var obj in rowData)
			{
				Assert.NotNull(obj);
			}
		}

		[Fact]
		public void TextReader()
		{
			using var tr = File.OpenText("Data\\Binary.csv");
			var csv = CsvDataReader.Create(tr);
			var buf = new char[32];
			while (csv.Read())
			{
				var idx = 0;
				int len;
				while ((len = (int)csv.GetChars(1, idx, buf, 0, buf.Length)) != 0)
				{
					idx += len;
				}

				Assert.True(idx > 0);
			}
		}

		[Fact]
		public void LineEndings()
		{
			using var tr = new StringReader("Id\r1\r\n2\n3\n\r");
			var csv = CsvDataReader.Create(tr);
			Assert.Equal("Id", csv.GetName(0));
			Assert.True(csv.Read());
			Assert.Equal("1", csv.GetString(0));
			Assert.True(csv.Read());
			Assert.Equal("2", csv.GetString(0));
			Assert.True(csv.Read());
			Assert.Equal("3", csv.GetString(0));
			Assert.True(csv.Read());
			Assert.Equal("", csv.GetString(0));
			Assert.False(csv.Read());
		}

		[Fact]
		public void MiscCoverage()
		{
			using var tr = new StringReader("Id,Name,Value");
			var csv = CsvDataReader.Create(tr);
			Assert.False(csv.HasRows);
			Assert.Equal(0, csv.Depth);
			Assert.False(csv.IsClosed);
			Assert.Equal(-1, csv.RecordsAffected);
		}

		[Fact]
		public void Boolean1()
		{
			using var tr = new StringReader("Bool\nT\nF\nX\n");
			var opts = new CsvDataReaderOptions()
			{
				TrueString = "t",
				FalseString = "f",
			};
			var csv = CsvDataReader.Create(tr, opts);
			Assert.True(csv.Read());
			Assert.True(csv.GetBoolean(0));
			Assert.True(csv.Read());
			Assert.False(csv.GetBoolean(0));
			Assert.True(csv.Read());
			Assert.Throws<FormatException>(() => csv.GetBoolean(0));
		}

		[Fact]
		public void Boolean2()
		{
			using var tr = new StringReader("Bool\nT\nF\nX\n");
			var opts = new CsvDataReaderOptions()
			{
				TrueString = "t",
				FalseString = null,
			};
			var csv = CsvDataReader.Create(tr, opts);
			Assert.True(csv.Read());
			Assert.True(csv.GetBoolean(0));
			Assert.True(csv.Read());
			Assert.False(csv.GetBoolean(0));
			Assert.True(csv.Read());
			Assert.False(csv.GetBoolean(0));
		}

		[Fact]
		public void Boolean3()
		{
			using var tr = new StringReader("Bool\nT\nF\nX\n");
			var opts = new CsvDataReaderOptions()
			{
				TrueString = null,
				FalseString = "f",
			};
			var csv = CsvDataReader.Create(tr, opts);
			Assert.True(csv.Read());
			Assert.True(csv.GetBoolean(0));
			Assert.True(csv.Read());
			Assert.False(csv.GetBoolean(0));
			Assert.True(csv.Read());
			Assert.True(csv.GetBoolean(0));
		}

		[Fact]
		public void Date1()
		{
			using var tr = new StringReader("Date\n20200803\n20200804\n20200805\n");
			var opts = new CsvDataReaderOptions()
			{
				DateFormat = "yyyyMMdd"
			};
			var csv = CsvDataReader.Create(tr, opts);
			Assert.True(csv.Read());
			Assert.Equal(new DateTime(2020, 8, 3), csv.GetDateTime(0));
			Assert.True(csv.Read());
			Assert.Equal(new DateTime(2020, 8, 4), csv.GetDateTime(0));
			Assert.True(csv.Read());
			Assert.Equal(new DateTime(2020, 8, 5), csv.GetDateTime(0));
		}


		//[Fact]
		//public void DeDupeString()
		//{
		//	using var tr = new StringReader("Name\nABC\nABC\nBCD\nBCD\n");
		//	var opts = new CsvDataReaderOptions()
		//	{
		//		PoolStrings = true
		//	};
		//	var csv = CsvDataReader.Create(tr, opts);
		//	Assert.True(csv.Read());
		//	var str1 = csv.GetString(0);
		//	Assert.Equal("ABC", str1);
		//	Assert.True(csv.Read());
		//	var str2 = csv.GetString(0);
		//	Assert.Equal("ABC", str2);
		//	Assert.Same(str1, str2);

		//	Assert.True(csv.Read());
		//	str1 = csv.GetString(0);
		//	Assert.Equal("BCD", str1);
		//	Assert.True(csv.Read());
		//	str2 = csv.GetString(0);
		//	Assert.Equal("BCD", str2);
		//	Assert.Same(str2, str2);
		//}



		[Fact]
		public void AutoDetect1()
		{
			using var tr = new StringReader("A;B;C;D;E\n1;2;3;4;5\n");
			var csv = CsvDataReader.Create(tr);
			Assert.Equal(5, csv.FieldCount);
			Assert.Equal("A", csv.GetName(0));
			Assert.Equal("D", csv.GetName(3));
		}

		[Fact]
		public void AutoDetect2()
		{
			using var tr = new StringReader("A|B,(b)|C|D|E\n1|2|3|4|5\n");
			var csv = CsvDataReader.Create(tr);
			Assert.Equal(5, csv.FieldCount);
			Assert.Equal("A", csv.GetName(0));
			Assert.Equal("D", csv.GetName(3));
		}

		[Fact]
		public void MissingFieldTest()
		{
			using var tr = new StringReader("a,b,c\n1,2,3\n4,5\n6,7,8\n");
			var csv = CsvDataReader.Create(tr);
			Assert.True(csv.Read());
			Assert.True(csv.Read());
			Assert.Equal("4", csv.GetString(0));
			Assert.Equal("5", csv.GetString(1));
			Assert.Equal("", csv.GetString(2));
			Assert.False(csv.IsDBNull(0));
			Assert.False(csv.IsDBNull(1));
			Assert.False(csv.IsDBNull(2));
			Assert.Throws<ArgumentOutOfRangeException>(() => csv.GetString(-1));
			Assert.Throws<ArgumentOutOfRangeException>(() => csv.GetString(3));
		}

		[Fact]
		public void CustomFormatTest()
		{
			var schema = Schema.Parse("Name,Date:DateTime{yyyyMMdd}");
			var csvSchema = new CsvSchema(schema.GetColumnSchema());
			using var tr = new StringReader("Test,20200812");
			var csv = CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = csvSchema, HasHeaders = false });
			Assert.True(csv.Read());
			Assert.Equal("Test", csv.GetString(0));
			Assert.Equal(new DateTime(2020, 8, 12), csv.GetDateTime(1));
		}


		[Fact]
		public void EmptyStringTest()
		{
			using var tr = new StringReader("Name,Value\r\nTest,");
			var csv = CsvDataReader.Create(tr);
			Assert.True(csv.Read());
			Assert.Equal("Test", csv.GetString(0));
			Assert.False(csv.IsDBNull(1));
			Assert.Equal("", csv.GetValue(1));
		}

		[Fact]
		public void DbNullTest()
		{
			using var tr = new StringReader("Name,Value\r\nTest,");
			var csv = CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = CsvSchema.Nullable });
			Assert.True(csv.Read());
			Assert.Equal("Test", csv.GetString(0));
			Assert.True(csv.IsDBNull(1));
			Assert.Equal(DBNull.Value, csv.GetValue(1));
		}

		[Fact]
		public void EmptyString2Test()
		{
			using var tr = new StringReader("Name,Value\r\nTest");
			var csv = CsvDataReader.Create(tr);
			Assert.True(csv.Read());
			Assert.Equal("Test", csv.GetString(0));
			Assert.False(csv.IsDBNull(1));
			Assert.Equal("", csv.GetValue(1));
		}

		[Fact]
		public void DbNull2Test()
		{
			using var tr = new StringReader("Name,Value\r\nTest");
			var csv = CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = CsvSchema.Nullable });
			Assert.True(csv.Read());
			Assert.Equal("Test", csv.GetString(0));
			Assert.True(csv.IsDBNull(1));
			Assert.Equal(DBNull.Value, csv.GetValue(1));
		}


		[Fact]
		public void RowFieldCountTest()
		{
			using var tr = new StringReader("Name,Value\r\nTest");
			var csv = CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = CsvSchema.Nullable });
			Assert.True(csv.Read());
			Assert.Equal("Test", csv.GetString(0));
			Assert.Equal(2, csv.FieldCount);
			Assert.Equal(1, csv.RowFieldCount);
		}

		[Fact]
		public void RowFieldCount2Test()
		{
			using var tr = new StringReader("Name,Value\r\n\r\n");
			var csv = CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = CsvSchema.Nullable });
			Assert.True(csv.Read());
			Assert.Equal(2, csv.FieldCount);
			Assert.Equal(1, csv.RowFieldCount);
		}

		[Fact]
		public void RowFieldCount3Test()
		{
			using var tr = new StringReader("Name,Value\r\n,,\r\n");
			var csv = CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = CsvSchema.Nullable });
			Assert.True(csv.Read());
			Assert.Equal(2, csv.FieldCount);
			Assert.Equal(3, csv.RowFieldCount);
		}

		[Fact]
		public void BadQuote()
		{
			using var tr = new StringReader("Name,Value\r\nA\"\"B,\"And\"more,\r\n");
			var csv = CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = CsvSchema.Nullable });
			Assert.True(csv.Read());
			Assert.Equal("A\"\"B", csv.GetString(0));
			Assert.Equal("Andmore", csv.GetString(1));
		}

		[Fact]
		public void WhitespaceAsNull()
		{
			var schema = new TypedCsvSchema();
			schema.Add(0, typeof(string));
			schema.Add(1, typeof(int?));

			using var tr = new StringReader("Name,Value\nA,1\nB, \nC,3");
			var csv = CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = schema });
			csv.Read();
			Assert.False(csv.IsDBNull(1));
			Assert.Equal(1, csv.GetInt32(1));
			csv.Read();
			Assert.True(csv.IsDBNull(1));
			Assert.Throws<FormatException>(() => csv.GetInt32(1));
			csv.Read();
			Assert.False(csv.IsDBNull(1));
			Assert.Equal(3, csv.GetInt32(1));
		}

		[Fact]
		public void CultureWithCommaDecimal()
		{
			using var reader = new StringReader("Name;Value1;Value2\nTest;2,08;0,82\nB;1;2");

			var schema = Schema.Parse("Name,Value1:float,Value2:float");

			var options = new CsvDataReaderOptions
			{
				Schema = new CsvSchema(schema),
				Culture = CultureInfo.GetCultureInfoByIetfLanguageTag("it-IT")
			};
			var csv = CsvDataReader.Create(reader, options);
			Assert.True(csv.Read());
			Assert.Equal(2.08f, csv.GetFloat(1));
			Assert.Equal(0.82f, csv.GetFloat(2));
			Assert.True(csv.Read());
			Assert.Equal(1, csv.GetFloat(1));
			Assert.Equal(2, csv.GetFloat(2));
		}
		
		[Fact]
		public void Binary2()
		{
			using var reader = new StringReader("Name,Value\r\nrow1,abcdefgh");
			var csv = CsvDataReader.Create(reader);
			csv.Read();
			var buf = new byte[0x100];
			var len = csv.GetBytes(1, 0, null, 0, 0);
			Assert.Equal(6, len);
			len = csv.GetBytes(1, 0, buf, 0, buf.Length);
			Assert.Equal(6, len);

		}
	}
}
