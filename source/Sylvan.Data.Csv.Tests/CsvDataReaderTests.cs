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
		public void Simple1()
		{
			var csv = CsvDataReader.Create(new StringReader("ABC,DEF\n1,2\n3,4\n5,6\n5,6\n5,6\n5,6\n5,6\n5,6\n5,6\n5,6\n"));
			while (csv.Read()) ;
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
			using var csv = await CsvDataReader.CreateAsync("Data/Simple.csv");
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
			using (var reader = File.OpenText("Data/Binary.csv"))
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
			using (var reader = File.OpenText("Data/Binary.csv"))
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
			using (var reader = File.OpenText("Data/PartialRecord.csv"))
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
			using (var reader = File.OpenText("Data/Quote.csv"))
			{
				var csv = await CsvDataReader.CreateAsync(reader);
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
				Assert.Equal($"Very{Environment.NewLine}Low{Environment.NewLine}", csv[2]);
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
			using (var reader = File.OpenText("Data/DataOnly.csv"))
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

			using (var reader = File.OpenText("Data/DataOnly.csv"))
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
			var csv = await CsvDataReader.CreateAsync(tr);
			await csv.ProcessAsync();
		}

		[Fact]
		public void Sync()
		{
			using (var reader = File.OpenText("Data/Simple.csv"))
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
			using (var reader = File.OpenText("Data/Simple.csv"))
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
			using (var reader = File.OpenText("Data/Simple.csv"))
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
			using (var reader = File.OpenText("Data/Simple.csv"))
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
			using var tr = File.OpenText("Data/Binary.csv");
			var csv = CsvDataReader.Create(tr, opts);
			csv.Read();
			Assert.Throws<CsvRecordTooLargeException>(() => csv.Read());
		}

		[Fact]
		public void NextResult()
		{
			using var tr = File.OpenText("Data/Binary.csv");
			var csv = CsvDataReader.Create(tr);
			Assert.False(csv.NextResult());
			Assert.False(csv.Read());
		}

		[Fact]
		public async Task NextResultAsync()
		{
			using var tr = File.OpenText("Data/Binary.csv");
			var csv = CsvDataReader.Create(tr);
			Assert.False(await csv.NextResultAsync());
			Assert.False(await csv.ReadAsync());
		}

		CsvDataReader GetTypedReader()
		{
			var tr = File.OpenText("Data/Types.csv");
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
			using var tr = File.OpenText("Data/Binary.csv");
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

		[Fact]
		public void Date2()
		{
			using var tr = new StringReader("Date\n03/24/2021\n3/3/2021\n12/3/2021\n");

			var csv = CsvDataReader.Create(tr);
			DateTime d;
			Assert.True(csv.Read());
			d = csv.GetDateTime(0);
			Assert.Equal(new DateTime(2021, 3, 24), d);
			Assert.Equal(DateTimeKind.Unspecified, d.Kind);
			Assert.True(csv.Read());
			d = csv.GetDateTime(0);
			Assert.Equal(new DateTime(2021, 3, 3), d);
			Assert.Equal(DateTimeKind.Unspecified, d.Kind);
			Assert.True(csv.Read());
			d = csv.GetDateTime(0);
			Assert.Equal(new DateTime(2021, 12, 3), d);
			Assert.Equal(DateTimeKind.Unspecified, d.Kind);
		}

		[Fact]
		public void Date3()
		{
			using var tr = new StringReader("Date\n2020-01-01T00:00:00Z\n2020-01-01T00:00:00");
			var csv = CsvDataReader.Create(tr);
			DateTime d;
			Assert.True(csv.Read());
			d = csv.GetDateTime(0);
			Assert.Equal(DateTimeKind.Utc, d.Kind);
			Assert.Equal(new DateTime(2020, 1, 1), d);
			Assert.True(csv.Read());
			d = csv.GetDateTime(0);
			Assert.Equal(DateTimeKind.Unspecified, d.Kind);
			Assert.Equal(new DateTime(2020, 1, 1), d);
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
		public void BadQuoteFirstRow()
		{
			using var tr = new StringReader("Name,Value\nA,\"B\"C\n");
			var ex = Assert.Throws<CsvFormatException>(() => CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = CsvSchema.Nullable }));
			Assert.Equal(1, ex.RowNumber);
		}

		[Fact]
		public void BadQuoteHeader()
		{
			using var tr = new StringReader("Name,\"Va\"lue\nA,\"B\"C\n");
			var ex = Assert.Throws<CsvFormatException>(() => CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = CsvSchema.Nullable }));
			Assert.Equal(0, ex.RowNumber);
		}

		[Fact]
		public void BadQuote()
		{
			using var tr = new StringReader("Name,Value\nA\"\"B,b,\nA\"\"B,\"And\"more,\n");
			var csv = CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = CsvSchema.Nullable });
			Assert.True(csv.Read());
			Assert.Equal("A\"\"B", csv.GetString(0));
			Assert.Throws<CsvFormatException>(() => csv.Read());
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

		[Theory]
		[InlineData("N,V\na\\,b,c\n", "a,b", "c")]
		[InlineData("N,V\na\\\nb,c\n", "a\nb", "c")]
		[InlineData("N,V\na\\\r\nb\n", "a\r\nb", "")]
		[InlineData("N,V\na\\\r\nb", "a\r\nb", "")]
		public void ImpliedQuote(string input, string a, string b)
		{
			using var reader = new StringReader(input);
			var options =
				new CsvDataReaderOptions
				{
					CsvStyle = CsvStyle.Escaped,
					Escape = '\\'
				};

			var csv = CsvDataReader.Create(reader, options);
			Assert.True(csv.Read());
			Assert.Equal(a, csv.GetString(0));
			Assert.Equal(b, csv.GetString(1));
			Assert.False(csv.Read());
		}

		[Fact]
		public void BinaryB64Err()
		{
			using var reader = new StringReader("Name,Value\r\nrow1,####");
			var csv = CsvDataReader.Create(reader);
			csv.Read();
			var buf = new byte[0x100];
			var len = csv.GetBytes(1, 0, null, 0, 0);
			Assert.Equal(3, len);
			Assert.Throws<FormatException>(() => csv.GetBytes(1, 0, buf, 0, buf.Length));
		}

		[Fact]
		public void BinaryHex()
		{
			var schema = Schema.Parse(",:binary{hex}");
			using var reader = new StringReader("Name,Value\r\nrow1,0102030405060708");
			var csv = CsvDataReader.Create(reader, new CsvDataReaderOptions { Schema = new CsvSchema(schema) });
			csv.Read();
			var len = csv.GetBytes(1, 0, null, 0, 0);
			Assert.Equal(8, len);
			var buf = new byte[len];
			len = csv.GetBytes(1, 0, buf, 0, buf.Length);
			Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, buf);
		}

		[Fact]
		public void BinaryHex2()
		{
			var schema = Schema.Parse(",:binary{hex}");
			using var reader = new StringReader("Name,Value\r\nrow1,012");
			var csv = CsvDataReader.Create(reader, new CsvDataReaderOptions { Schema = new CsvSchema(schema) });
			csv.Read();
			Assert.Throws<FormatException>(() => csv.GetBytes(1, 0, null, 0, 0));

			var buf = new byte[2];
			Assert.Throws<FormatException>(() => csv.GetBytes(1, 0, buf, 0, buf.Length));
		}

		[Fact]
		public void BinaryHex3()
		{
			var schema = Schema.Parse(",:binary{hex}");
			using var reader = new StringReader("Name,Value\r\nrow1,zz");
			var csv = CsvDataReader.Create(reader, new CsvDataReaderOptions { Schema = new CsvSchema(schema) });
			csv.Read();
			var len = csv.GetBytes(1, 0, null, 0, 0);
			Assert.Equal(1, len);
			var buf = new byte[len];
			Assert.Throws<FormatException>(() => csv.GetBytes(1, 0, buf, 0, buf.Length));
		}

		[Fact]
		public void BinaryHexPrefix()
		{

			using var reader = new StringReader("Name,Value\r\nrow1,0x01020304");
			var csv = CsvDataReader.Create(reader, new CsvDataReaderOptions { BinaryEncoding = BinaryEncoding.Hexadecimal });
			csv.Read();
			var len = (int)csv.GetBytes(1, 0, null, 0, 0);
			Assert.Equal(4, len);
			var buf = new byte[len];
			csv.GetBytes(1, 0, buf, 0, len);
			Assert.Equal(new byte[] { 1, 2, 3, 4 }, buf);

		}

		class VersionObj
		{
			public string Name { get; private set; }
			public int Num { get; private set; }
			public Version Ver { get; private set; }

		}

		[Fact]
		public void Ver()
		{
			using var reader = new StringReader("Name,Num,Ver\r\nrow1,12,1.2.3.4");
			var csv = CsvDataReader.Create(reader);
			var binder = DataBinder.Create<VersionObj>(csv);
			while (csv.Read())
			{
				var f = new VersionObj();
				binder.Bind(csv, f);
			}
		}


		class BinaryObj
		{
			public string Name { get; private set; }
			public byte[] Data { get; private set; }
		}

		[Fact]
		public void BinBindTest()
		{
			var schema = Schema.Parse(",:binary{hex}");
			using var reader = new StringReader("Name,Data\r\nrow1,0102030405060708");
			var csv = CsvDataReader.Create(reader, new CsvDataReaderOptions { Schema = new CsvSchema(schema) });
			var binder = DataBinder.Create<BinaryObj>(csv);
			while (csv.Read())
			{
				var f = new BinaryObj();
				binder.Bind(csv, f);
			}
		}

		[Fact]
		public void UnknownBinaryTest()
		{
			var schema = Schema.Parse(",:binary{unk}");
			using var reader = new StringReader("Name,Data\r\nrow1,0102030405060708");
			var csv = CsvDataReader.Create(reader, new CsvDataReaderOptions { Schema = new CsvSchema(schema) });
			while (csv.Read())
			{
				Assert.Throws<NotSupportedException>(() => csv.GetValue(1));
			}
		}

		[Fact]
		public void QuoteHandling()
		{
			using var reader = new StringReader("Name\r\n\b\r\n\"quoted\"field,");
			var csv = CsvDataReader.Create(reader);
			Assert.True(csv.Read());
			Assert.Throws<CsvFormatException>(() => csv.Read());
		}

		[Fact]
		public void MRS()
		{
			using var reader = new StringReader("A,B\ntest,1\n\nC,D\ntest2,2");
			var csv = CsvDataReader.Create(reader, new CsvDataReaderOptions { ResultSetMode = ResultSetMode.MultiResult });
			Assert.Equal("A", csv.GetName(0));
			Assert.Equal("B", csv.GetName(1));
			Assert.True(csv.Read());
			Assert.Equal("test", csv.GetString(0));
			Assert.False(csv.Read());
			Assert.True(csv.NextResult());
			Assert.True(csv.Read());
			Assert.Equal("C", csv.GetName(0));
			Assert.Equal("D", csv.GetName(1));
			Assert.Equal("test2", csv.GetString(0));
			Assert.Equal("2", csv.GetString(1));
			Assert.False(csv.Read());
			Assert.False(csv.NextResult());
		}

		[Fact]
		public void MRS2()
		{
			using var reader = new StringReader("A,B\n1,2\nC,D,E\n3,4,5");
			var csv = CsvDataReader.Create(reader, new CsvDataReaderOptions { ResultSetMode = ResultSetMode.MultiResult });
			Assert.Equal("A", csv.GetName(0));
			Assert.Equal("B", csv.GetName(1));
			Assert.True(csv.Read());
			Assert.Equal("1", csv.GetString(0));
			Assert.Equal("2", csv.GetString(1));
			Assert.False(csv.Read());
			Assert.True(csv.NextResult());
			Assert.True(csv.Read());
			Assert.Equal("C", csv.GetName(0));
			Assert.Equal("D", csv.GetName(1));
			Assert.Equal("E", csv.GetName(2));
			Assert.Equal("3", csv.GetString(0));
			Assert.Equal("4", csv.GetString(1));
			Assert.Equal("5", csv.GetString(2));
			Assert.False(csv.Read());
			Assert.False(csv.NextResult());
		}

		[Fact]
		public void CommentTest()
		{
			using var reader = new StringReader("#comment\na,b,c\n1,2,3\n4,5,6");
			var csv = CsvDataReader.Create(reader);
			Assert.True(csv.Read());
			Assert.Equal(3, csv.FieldCount);
			Assert.True(csv.Read());
			Assert.False(csv.Read());
		}

		[Fact]
		public void Comment2Test()
		{
			using var reader = new StringReader("a,b,c\n#comment\n1,2,3\n4,5,6");
			var csv = CsvDataReader.Create(reader);
			Assert.True(csv.Read());
			Assert.Equal(3, csv.FieldCount);
			Assert.True(csv.Read());
			Assert.False(csv.Read());
		}

		[Fact]
		public void Comment3Test()
		{
			using var reader = new StringReader("a,b,c\n1,2,3\n4,5,6\n#comment\n");
			var csv = CsvDataReader.Create(reader);
			Assert.True(csv.Read());
			Assert.Equal(3, csv.FieldCount);
			Assert.True(csv.Read());
			Assert.False(csv.Read());
		}

		[Fact]
		public void Comment4Test()
		{
			using var reader = new StringReader("a,b,c\n1,2,3\n4,5,6\n#comment");
			var csv = CsvDataReader.Create(reader);
			Assert.True(csv.Read());
			Assert.Equal(3, csv.FieldCount);
			Assert.True(csv.Read());
			Assert.False(csv.Read());
		}

		[Fact]
		public void Comment5Test()
		{
			using var reader = new StringReader("a,b,c\n1,#2,3\n4,5,6\n");
			var csv = CsvDataReader.Create(reader);
			Assert.True(csv.Read());
			Assert.Equal(3, csv.FieldCount);
			Assert.Equal("#2", csv.GetString(1));
			Assert.True(csv.Read());
			Assert.False(csv.Read());
		}

		[Theory]
		[InlineData("a,b,c\n#hello\n4,5,6\n", "hello")]
		[InlineData("a,b,c\n#\n4,5,6\n", "")]
		public void Comment6Test(string data, string result)
		{
			using var reader = new StringReader(data);
			string c = null;
			int commentCount = 0;
			Action<string> cb = (string comment) => { c = comment; commentCount++; };

			var csv = CsvDataReader.Create(reader, new CsvDataReaderOptions { CommentHandler = cb });
			Assert.True(csv.Read());
			Assert.Equal(1, commentCount);
			Assert.Equal(result, c);
			Assert.Equal(3, csv.FieldCount);
			Assert.False(csv.Read());
		}

		[Fact]
		public void InvalidHexTest()
		{
			var opts = new CsvDataReaderOptions { BinaryEncoding = BinaryEncoding.Hexadecimal, HasHeaders = false };
			var reader = CsvDataReader.Create(new StringReader("1,0102zz"), opts);
			reader.Read();
			Assert.Equal(1, reader.GetInt32(0));
			var buffer = new byte[16];
			Assert.Throws<FormatException>(() => reader.GetBytes(1, 0, buffer, 0, buffer.Length));
		}
	}
}
