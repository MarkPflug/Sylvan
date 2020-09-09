using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using Sylvan.Data;
using Sylvan.Data.Csv;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;

namespace Sylvan.BuildTools.Data
{
	public class SylvanDataCodeGen : Task
	{
		public ITaskItem[] InputFiles { get; set; }
		public string OutputPath { get; set; }

		public override bool Execute()
		{
			var pc = IdentifierStyle.PascalCase;
			bool success = true;
			foreach (var file in InputFiles)
			{
				var generatedSchemaFile = file.GetMetadata("GeneratedSchema");
				var userSchemaFile = file.GetMetadata("Schema");
				var codeFile = file.GetMetadata("GeneratedCode");
				var schemaFile =
					string.IsNullOrEmpty(userSchemaFile)
					? generatedSchemaFile
					: userSchemaFile;

				if (!string.IsNullOrEmpty(schemaFile))
				{
					var spec = File.ReadAllText(schemaFile);
					var schema = Schema.TryParse(spec);
					if (schema == null)
					{
						this.BuildEngine.LogErrorEvent(
							new BuildErrorEventArgs(
								"1234",
								"1234", schemaFile, 1, 1, 1, 1,
								"Failed to parse schema", null, null
							)
						);
						success = false;
					}
					var sw = new StringWriter();
					var typeName = pc.Convert(file.GetMetadata("filename"));
					sw.WriteLine("// File: " + file.ItemSpec);
					sw.WriteLine("using System;");
					sw.WriteLine("using Sylvan.Data.Csv;");
					sw.WriteLine("using Sylvan.Data;");
					sw.WriteLine("using System.Collections.Generic;");
					sw.WriteLine("using System.Collections.ObjectModel;");
					sw.WriteLine("using System.Data.Common;");

					sw.WriteLine("class " + typeName + "Record {");
					var colSchema = schema.GetColumnSchema();
					foreach (var col in colSchema)
					{
						var dt = col.DataType;
						var fullName = dt.FullName;
						sw.WriteLine("[ColumnOrdinal(" + col.ColumnOrdinal + ")]");
						if(!string.IsNullOrEmpty(col.ColumnName))
							sw.WriteLine("[ColumnName(\"" + col.ColumnName + "\")]");

						var memberName = string.IsNullOrWhiteSpace(col.ColumnName) ? "Column" + (col.ColumnOrdinal + 1) : pc.Convert(col.ColumnName);
						sw.WriteLine("public " + fullName + (col.AllowDBNull == true && dt.IsValueType ? "?" : "") + " " + memberName + " { get; set; }");

					}
					sw.WriteLine("}");

					var hasHeaders = colSchema.All(c => !string.IsNullOrEmpty(c.ColumnName));

					sw.WriteLine("class " + typeName + "Set {");
					sw.WriteLine("const string FileName = @\"" + file.ItemSpec + "\";");
					sw.WriteLine("const string SchemaSpec = \"" + schema.GetSchemaSpecification() + "\";");
					sw.WriteLine("static readonly ReadOnlyCollection<DbColumn> ColumnSchema = Sylvan.Data.Schema.TryParse(SchemaSpec).GetColumnSchema();");
					sw.WriteLine("static readonly ICsvSchemaProvider SchemaProvider = new CsvSchema(ColumnSchema);");


					sw.WriteLine("static readonly CsvDataReaderOptions DefaultOptions = new CsvDataReaderOptions {");
					sw.WriteLine("HasHeaders = " + (hasHeaders ? "true" : "false") + ",");
					sw.WriteLine("Schema = SchemaProvider,");					

					sw.WriteLine("};");

					sw.WriteLine("public static IEnumerable<" + typeName + "Record> Read() { return Read(FileName, DefaultOptions); }");

					sw.WriteLine("public static IEnumerable<" + typeName + "Record> Read(string filename, CsvDataReaderOptions opts) {");
					sw.WriteLine("var binder = new CompiledDataBinder<" + typeName + "Record>(ColumnSchema);");
					sw.WriteLine("var csv = CsvDataReader.Create(filename, opts);");

					sw.WriteLine("while(csv.Read()) {");
					sw.WriteLine("var item = new " + typeName + "Record();");
					sw.WriteLine("binder.Bind(csv, item);");
					sw.WriteLine("yield return item;");
					sw.WriteLine("}");

					sw.WriteLine("}");

					sw.WriteLine("}");

					var code = sw.ToString();
					File.WriteAllText(codeFile, code);
				}
			}
			return success;
		}
	}

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
					var analyzer = new SchemaAnalyzer(new SchemaAnalyzerOptions { AnalyzeRowCount = 1000 });
					var result = analyzer.Analyze(csv);
					var schema = result.GetSchema();
					var spec = new Schema(schema).GetSchemaSpecification(true);
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
				}
			}
			return success;
		}
	}
}

