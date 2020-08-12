using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using Sylvan.Data;
using Sylvan.Data.Csv;
using System.IO;

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
					sw.WriteLine("class " + typeName + "{");
					foreach(var col in schema.GetColumnSchema())
					{
						var dt = col.DataType;
						var fullName = dt.FullName;
						var memberName = pc.Convert(col.ColumnName);
						sw.WriteLine("public " + fullName + (col.AllowDBNull == true && dt.IsValueType ? "?" : "") + " " + memberName + " { get; set; }");

					}
					sw.WriteLine("}");

					File.WriteAllText(codeFile, sw.ToString());
				}
			}
			return success;
		}
	}

	public class SylvanDataGenerateSchema : Task
	{
		public ITaskItem[] InputFiles { get; set; }

		public override bool Execute()
		{
			foreach (var file in InputFiles)
			{
				var generatedSchemaFile = file.GetMetadata("GeneratedSchema");
				var csv = CsvDataReader.Create(file.ItemSpec);
				var analyzer = new SchemaAnalyzer();
				var result = analyzer.Analyze(csv);
				var schema = analyzer.GetSchema(result);
				var spec = new Schema(schema).GetSchemaSpecification(true);
				File.WriteAllText(generatedSchemaFile, spec);
			}
			return true;
		}
	}
}

