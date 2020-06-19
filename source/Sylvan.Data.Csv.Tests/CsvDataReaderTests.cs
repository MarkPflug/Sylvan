using System;
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
		public async Task Simple()
		{
			using (var reader = File.OpenText("Data\\Simple.csv"))
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
				Assert.True(csv.IsDBNull(3));
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
		public async Task DataOnly()
		{
			using (var reader = File.OpenText("Data\\DataOnly.csv"))
			{
				var csv = await CsvDataReader.CreateAsync(reader, new CsvDataReaderOptions { HasHeaders = false });
				Assert.Equal(4, csv.FieldCount);
				Assert.True(csv.HasRows);
				Assert.Equal(0, csv.RowNumber);
				Assert.Throws<InvalidOperationException>(() => csv.GetName(0));
				Assert.Throws<InvalidOperationException>(() => csv.GetOrdinal("Id"));
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
		public async Task HeadersOnly()
		{
			using (var reader = File.OpenText("Data\\HeadersOnly.csv"))
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
	}
}
