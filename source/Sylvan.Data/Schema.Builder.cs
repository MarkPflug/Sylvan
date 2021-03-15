using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace Sylvan.Data
{
	partial class Schema
	{
		/// <summary>
		/// Builder for creating Schema.
		/// </summary>
		public class Builder : IEnumerable<Column.Builder>
		{
			List<Column.Builder> columns;

			/// <summary>
			/// Creates a new Builder.
			/// </summary>
			public Builder()
			{
				this.columns = new List<Column.Builder>();
			}

			public int Count { get => this.columns.Count; }

			public Column.Builder this[int ordinal]
			{
				get
				{
					if (ordinal < 0 || ordinal >= columns.Count)
						throw new IndexOutOfRangeException();
					return columns[ordinal];
				}
			}

			/// <summary>
			/// Creates a new Builder.
			/// </summary>
			public Builder(IEnumerable<DbColumn> schema)
			{
				this.columns = new List<Column.Builder>();
				foreach(var col in schema)
				{
					this.columns.Add(new Column.Builder(col));
				}
			}

			public Builder Add(Column.Builder columnBuilder)
			{
				var ordinal = this.columns.Count;
				
				columnBuilder.BaseColumnOrdinal = columnBuilder.BaseColumnOrdinal ?? columnBuilder.ColumnOrdinal;
				columnBuilder.ColumnOrdinal = ordinal;

				this.columns.Add(columnBuilder);				
				return this;
			}

			public Builder Add<T>(string? name = null, bool allowNull = false) {
				var t = typeof(T);
				var underlying = Nullable.GetUnderlyingType(t);
				if(underlying != null) {
					t = underlying;
					allowNull = true;
				}
				var cb = new Column.Builder(name ?? "", t, allowNull);
				return Add(cb);
			}

			/// <summary>
			/// Builds an immutable Schema instance.
			/// </summary>
			public Schema Build()
			{
				var builders = this.columns;
				var cols = new Column[builders.Count];
				for (int i = 0; i < builders.Count; i++)
				{
					cols[i] = builders[i].Build();
				}

				return new Schema(cols);
			}

			public IEnumerator<Column.Builder> GetEnumerator()
			{
				foreach(var col in this.columns)
				{
					yield return col;
				}
			}

			IEnumerator IEnumerable.GetEnumerator()
			{
				return this.GetEnumerator();
			}
		}
	}
}
