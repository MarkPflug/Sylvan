using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using Sylvan.Data;
using System.IO;

namespace Sylvan.BuildTools.Data
{
	public class SylvanDataCodeGen : Task
	{
		public ITaskItem[] InputFiles { get; set; }
		public string OutputPath { get; set; }

		IdentifierStyle pc = IdentifierStyle.PascalCase;

		public override bool Execute()
		{
			
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
					var schema = Schema.Parse(spec);
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
					if (char.IsDigit(typeName[0]))
						typeName = "DataSet" + typeName;
					sw.WriteLine("// File: " + file.ItemSpec);
					sw.WriteLine("using System;");
					sw.WriteLine("using Sylvan.Data.Csv;");
					sw.WriteLine("using Sylvan.Data;");
					sw.WriteLine("using System.Collections.Generic;");
					sw.WriteLine("using System.Collections.ObjectModel;");
					sw.WriteLine("using System.Data;");
					sw.WriteLine("using System.Data.Common;");
					
					sw.WriteLine("class " + typeName + " {");
					var colSchema = schema.GetColumnSchema();
					int unnamedCounter = 1;
					//int unnamedSeriesCounter = 1;
					foreach (var col in colSchema)
					{
						if (col["IsSeries"] is true)
						{
							var name = col["SeriesName"] as string;

							var dt = col.DataType;
							var fullName = dt.FullName;

							var pattern = col["SeriesHeaderFormat"] as string;
							if (pattern != null)
							{
								sw.WriteLine("[ColumnSeries(\"" + pattern + "\")]");
							}

							var memberName = 
								string.IsNullOrWhiteSpace(name) 
								? "Values" 
								: GetColumnName(name);

							sw.WriteLine("public DateSeries<" + fullName + (col.AllowDBNull == true && dt.IsValueType ? "?" : "") + "> " + memberName + " { get; set; }");
						}
						else
						{
							var dt = col.DataType;
							var fullName = dt.FullName;
							sw.Write("[DataMember(Order = " + col.ColumnOrdinal);
							if (!string.IsNullOrEmpty(col.ColumnName))
							{
								sw.Write(", Name = \"" + col.ColumnName + "\"");
							}
							sw.WriteLine(")]");

							var memberName = 
								string.IsNullOrWhiteSpace(col.ColumnName) 
								? "Column" + (unnamedCounter++) 
								: GetColumnName(col.ColumnName);
							sw.WriteLine("public " + fullName + (col.AllowDBNull == true && dt.IsValueType ? "?" : "") + " " + memberName + " { get; set; }");
						}
					}

					sw.WriteLine("const string FileName = @\"" + file.GetMetadata("Filename") + file.GetMetadata("Extension") + "\";");
					sw.WriteLine("const string SchemaSpec = \"" + schema.ToString() + "\";");
					sw.WriteLine("static readonly ReadOnlyCollection<DbColumn> ColumnSchema = Sylvan.Data.Schema.Parse(SchemaSpec).GetColumnSchema();");
					sw.WriteLine("static readonly ICsvSchemaProvider SchemaProvider = new CsvSchema(ColumnSchema);");


					sw.WriteLine("static readonly CsvDataReaderOptions DefaultOptions = new CsvDataReaderOptions {");
					sw.WriteLine("Schema = SchemaProvider,");
					sw.WriteLine("BufferSize = 0x80000,");
					sw.WriteLine("};");

					sw.WriteLine("public static IEnumerable<" + typeName + "> Read(Action<IDataRecord,Exception> errorHandler = null) { return Read(FileName, DefaultOptions, errorHandler); }");

					sw.WriteLine("public static IEnumerable<" + typeName + "> Read(string filename, CsvDataReaderOptions opts, Action<IDataRecord,Exception> errorHandler = null) {");
					sw.WriteLine("using var csv = CsvDataReader.Create(filename, opts);");
					sw.WriteLine("var binder = DataBinder<" + typeName + ">.Create(ColumnSchema, csv.GetColumnSchema());");

					sw.WriteLine("while(csv.Read()) {");
					sw.WriteLine("var item = new " + typeName + "();");
					sw.WriteLine("try {");
					sw.WriteLine("binder.Bind(csv, item);");
					sw.WriteLine("} catch(Exception e) {");
					sw.WriteLine("if(errorHandler != null) {");
					sw.WriteLine("errorHandler(csv, e);");
					sw.WriteLine("} else {");
					sw.WriteLine("throw;");
					sw.WriteLine("}");
					sw.WriteLine("}");
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

		string GetColumnName(string col)
		{
			var name = pc.Convert(col);
			if (char.IsDigit(name[0]))
				name = "Column" + name;
			return name;
		}
	}
}
