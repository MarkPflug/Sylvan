using System.Data;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Text;
using Xunit;

namespace Sylvan.Data.XBase.Tests
{

	public class EncodingsFixture
	{
		public EncodingsFixture()
		{
			Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
		}
	}

	public class XBbaseDataReaderTests : IClassFixture<EncodingsFixture>
	{
		const string DataFileName = "data.zip";
		const string DBaseFileName = "data.dbf";
		const string DataSetUrl = "https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_20m.zip";

		readonly byte[] CachedData;

		public XBbaseDataReaderTests()
		{
			if (!File.Exists(DataFileName))
			{
				{
					using var oStream = File.OpenWrite(DataFileName);
					using var iStream = new HttpClient().GetStreamAsync(DataSetUrl).Result;
					iStream.CopyTo(oStream);
				}
				{
					using var stream = File.OpenRead(DataFileName);
					var za = new ZipArchive(stream, ZipArchiveMode.Read);
					var entry = za.GetEntry("cb_2018_us_county_20m.dbf");
					using var oStream = File.Create(DBaseFileName);
					using var iStream = entry.Open();
					iStream.CopyTo(oStream);
				}
			}
			CachedData = File.ReadAllBytes(DBaseFileName);
		}

		Stream GetDBaseStream()
		{
			return new MemoryStream(CachedData);
		}

		[Fact]
		public void TestBig()
		{
			using var stream = File.OpenRead(@"C:\users\mark\downloads\data\GU_PLSSFirstDivision.dbf");
			var r = XBaseDataReader.Create(stream);
			var schema = r.GetColumnSchema();

			var a = new SchemaAnalyzer();
			var result = a.Analyze(r);
			r.Process();
		}

		[Fact]
		public void TestOnline()
		{
			var fileName = @"C:\Users\Mark\Downloads\cb_2018_us_cd116_20m.zip";
			var za = ZipFile.OpenRead(fileName);
			var entry = za.GetEntry(Path.GetFileNameWithoutExtension(fileName) + ".dbf");
			using var stream = entry.Open();
			var dr = XBaseDataReader.Create(stream);

			var dt = new DataTable();
			dt.TableName = Path.GetFileNameWithoutExtension(fileName);
			dt.Load(dr);
			var sw = new StringWriter();
			dt.WriteXml(sw);
			var str = sw.ToString();
		}

		[Fact]
		public void Test1()
		{
			using var stream = GetDBaseStream();
			var r = XBaseDataReader.Create(stream);
			Process(r);
		}

		[Fact]
		public void TestEnc()
		{
			using var stream = File.OpenRead(@"Data/vc2.dbf");
			var r = XBaseDataReader.Create(stream);
			Process(r);
		}

		[Fact]
		public void TestZip()
		{
			using var zs = File.OpenRead(DataFileName);
			var za = new ZipArchive(zs, ZipArchiveMode.Read);
			var entry = za.GetEntry("cb_2018_us_county_20m.dbf");

			using var stream = entry.Open();
			var r = XBaseDataReader.Create(stream);
			Process(r);
		}

		[Fact]
		public void Test2()
		{
			// TODO: add option to skip unknown types.
			Proc("data/FolderRoot.dbf");
		}

		[Fact]
		public void Test3()
		{
			Proc("data/data2.dbf", "data/data2.fpt");
		}

		[Fact]
		public void Numbers()
		{
			Proc("data/numbers.dbf");
		}

		[Fact]
		public void Numbers2()
		{
			Proc("data/number2.dbf");
		}

		[Fact]
		public void Varchar()
		{
			Proc("data/nulltest.dbf");
		}

		[Fact]
		public void MemoTest()
		{
			Proc("data/nulltest.dbf");
		}

		[Fact]
		public void A()
		{
			var ds = File.OpenRead(@"C:\users\mark\desktop\memobintest.dbf");
			var ms = File.OpenRead(@"C:\users\mark\desktop\memobintest.fpt");
			var dr = XBaseDataReader.Create(ds, ms);
			while (dr.Read())
			{
				var str = dr.GetString(0);

				var mss = new MemoryStream();
				var bin = dr.GetStream(1);
				bin.CopyTo(mss);
				var ss = Encoding.ASCII.GetString(mss.GetBuffer());
				
			}
		}

		void Proc(string name, string memoName = null)
		{
			using var stream = File.OpenRead(name);
			using var memoStream = memoName == null ? null : File.OpenRead(memoName);
			var r = XBaseDataReader.Create(stream, memoStream);
			Process(r);
		}

		void Process(XBaseDataReader r) { 
			var schema = r.GetColumnSchema();
			var ss = new Schema(r);
			var spec = ss.ToString();
			var c = 0;
			var sb = new StringBuilder();
			while (r.Read())
			{
				r.ProcessRecord();
				c++;
			}
		}
	}
}
