using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Sylvan.Data
{
	public sealed class SchemaAnalyzerOptions
	{
		internal static readonly SchemaAnalyzerOptions Default = new SchemaAnalyzerOptions();

		public SchemaAnalyzerOptions()
		{
			this.AnalyzeRowCount = 1000;
		}

		public int AnalyzeRowCount { get; set; }
	}

	public sealed class SchemaAnalyzer
	{
		int rowCount;

		public SchemaAnalyzer(SchemaAnalyzerOptions? options = null)
		{
			options ??= SchemaAnalyzerOptions.Default;
			this.rowCount = options.AnalyzeRowCount;
		}

		public DbColumn[] Analyze(DbDataReader dataReader) {
			throw new NotImplementedException();
		}
	}
}
