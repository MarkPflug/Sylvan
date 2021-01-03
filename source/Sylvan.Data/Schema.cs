using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Sylvan.Data
{
	/// <summary>
	/// Provides schema information for data.
	/// </summary>
	public sealed partial class Schema : IEnumerable<Schema.Column>, IDbColumnSchemaGenerator
	{

		internal const string DateSeriesMarker = "{Date}";
		internal const string IntegerSeriesMarker = "{Integer}";

		// types: byte,int16,int32,int64,float,double,decimal,string,binary,date,datetime,
		// Id:int;
		// FirstName:string[32]?;
		// LastName:string[32]?;
		// *:double?;
		Column[] columns;

		private Schema(IEnumerable<Column> cols)
		{
			this.columns = cols.ToArray();
		}

		public static implicit operator ReadOnlyCollection<DbColumn>(Schema schema)
		{
			return new ReadOnlyCollection<DbColumn>(schema.columns);
		}

		/// <summary>
		/// Creates a Schema from the schema of an existing data reader.
		/// </summary>
		/// <param name="dataReader">The data reader to use as a schema template.</param>
		public Schema(DbDataReader dataReader) : this(dataReader.GetColumnSchema()) { }

		/// <summary>
		/// Creates a Schema from an existing schema.
		/// </summary>
		/// <param name="schema">The schema to use as a template.</param>
		public Schema(ReadOnlyCollection<DbColumn> schema)
		{
			this.columns =
				schema
				.Select(c => c as Column ?? new Column.Builder(c).Build())
				.ToArray();
		}

		/// <inheritdoc/>
		public override string ToString()
		{
			return SimpleSchemaSerializer.SingleLine.GetSchemaSpec(this);
		}

		public static Schema Parse(string spec)
		{
			return SimpleSchemaSerializer.SingleLine.Parse(spec);
		}

		public ReadOnlyCollection<DbColumn> GetColumnSchema()
		{
			return new ReadOnlyCollection<DbColumn>(this.columns);
		}

		public IEnumerator<Column> GetEnumerator()
		{
			return this.columns.AsEnumerable().GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.GetEnumerator();
		}
	}
}
