using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using Sylvan.Data;
using Sylvan.Data.Csv;
using System;
using System.Collections.Generic;
using System.IO;

namespace Sylvan.BuildTools.Data
{
	public class SylvanDataGenerateSchema : Task
	{
		public ITaskItem[] InputFiles { get; set; }


		static bool HasHeaders(string csvFile)
		{
			var opts = new CsvDataReaderOptions { HasHeaders = false };
			using var csv = CsvDataReader.Create(csvFile, opts);
			if (csv.Read())
			{
				var hs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
				for (int i = 0; i < csv.FieldCount; i++)
				{
					var str = csv.GetString(i);
					if (string.IsNullOrWhiteSpace(str))
						return false;
					if (!hs.Add(str))
						return false;
				}
			}
			return true;
		}

		public override bool Execute()
		{
			bool success = true;
			foreach (var file in InputFiles)
			{
				var generatedSchemaFile = file.GetMetadata("GeneratedSchema");
				var csvFile = file.GetMetadata("FullPath");
				try
				{
					bool hasHeaders = HasHeaders(csvFile);
					using var csv = CsvDataReader.Create(csvFile, new CsvDataReaderOptions { HasHeaders = hasHeaders });
					var analyzer = new SchemaAnalyzer(new SchemaAnalyzerOptions { AnalyzeRowCount = 10000 });
					var result = analyzer.Analyze(csv);
					var schema = result.GetSchema();
					var spec = schema.ToString();
					File.WriteAllText(generatedSchemaFile, spec);
				}
				catch (Exception e)
				{
					this.BuildEngine.LogErrorEvent(
							   new BuildErrorEventArgs(
								   "1234",
								   "1234", csvFile, 1, 1, 1, 1,
								   e.Message, null, null
							   )
						   );
					success = false;
					throw;
				}
			}
			return success;
		}
	}
}
