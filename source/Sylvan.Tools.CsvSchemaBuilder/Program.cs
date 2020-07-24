using Sylvan.Data;
using Sylvan.Data.Csv;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace Sylvan.Tools.CsvSchemaBuilder
{
	class Program
	{
		// TOOD: resourceify this?
		const string CommonHeaders = "id,name,code,type,kind,class,date,address,zip,city,state,country,amount,value,text,description,price,quantity";
		
		static void View(DbDataReader dr, int code)
		{
			var ids = new HashSet<int>();

			var c = new Dictionary<string, int>();

			HashSet<int> seen = new HashSet<int>();

			while (dr.Read())
			{
				var id = dr.GetInt32(0);						

				var state = dr.GetString(3);

				if (c.TryGetValue(state, out int cc))
				{
					c[state] = cc + 1;
				}
				else
				{
					c.Add(state, 1);
				}
			}

			foreach (var kvp in c.OrderBy(k => k.Key))
			{
				Console.WriteLine($"{kvp.Key,20} {kvp.Value}");
			}
		}

		static void Main(string[] args)
		{
			new SqlServerLoadTest().BulkLoad2();
		}

		static void F() { 
			//var file = args[0];
			string file = @"C:\Users\Mark\Desktop\PGYR18_P011720.ZIP";
			using var s = File.OpenRead(file);
			var za = new ZipArchive(s, ZipArchiveMode.Read);
			var sa = new SchemaAnalyzer();
			foreach(var entry in za.Entries)
			{
				Console.WriteLine(entry.Name);
				using var tr = new StreamReader(entry.Open());
				var data = CsvDataReader.Create(tr);
				var sw = Stopwatch.StartNew();
				int i = 0;
				while (data.Read())
				{
					i++;
				}
				sw.Stop();
				Console.WriteLine(i + " " + sw.Elapsed.ToString());
				//var r = sa.Analyze(data);
			}

		}

		void Analyze(string file) {
			char delimiter, quote;
			{
				using var tr = File.OpenText(file);
				(delimiter, quote) = IdentifyFormat(tr);
				Console.WriteLine("Delimiter: " + delimiter);
				Console.WriteLine("Quote: " + quote);
			}
			{
				using var tr = File.OpenText(file);
				var opts =
					new CsvDataReaderOptions() {
						Delimiter = delimiter,
						Quote = quote
					};

				var dr = CsvDataReader.Create(tr, opts);
				WriteHeaders(dr);

				var analyzer = new SchemaAnalyzer();
				var cols = analyzer.Analyze(dr);
			}
			
		}

		static void WriteHeaders(DbDataReader reader)
		{
			for (int i = 0; i < reader.FieldCount; i++)
			{
				Console.WriteLine(reader.GetName(i));
			}
		}

		static void WriteRecord(DbDataReader reader, int c)
		{
			for (int i = 0; i < c/*reader.FieldCount*/; i++)
			{
				Console.WriteLine($"{reader.GetName(i),20} {reader.GetString(i)}");
			}
			Console.WriteLine("----------------");
		}

		static (char delimiter, char quote) IdentifyFormat(TextReader tr)
		{
			var buffer = new char[0x100];
			var len = tr.ReadBlock(buffer, 0, buffer.Length);
			var counters = new int[128];
			for (int i = 0; i < len; i++)
			{
				var c = buffer[i];
				if (c > counters.Length)
				{
					continue;
				}
				if (c == '\r' || c == '\n') break;
				counters[c]++;
			}
			var charCounts = counters.Select((c, i) => new { Count = c, Char = (char)i, IsDelim = IsPotentialDelimiter((char)i) });

			var ldCount = charCounts.Where(d => !d.IsDelim);

			var delimiters =
				charCounts
				.Where(d => d.IsDelim)
				.OrderByDescending(d => d.Count)
				.ToArray();

			var delimiter = delimiters.FirstOrDefault()?.Char;

			if (delimiter == null)
				throw new InvalidDataException();

			return (delimiter.Value, '\"');
		}

		static bool IsPotentialDelimiter(char c)
		{
			return c < 128 && char.IsLetterOrDigit(c) == false && c != '_' && c != ' ';
		}
	}
}
