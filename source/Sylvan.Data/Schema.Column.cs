using System;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data;

partial class Schema
{
	public sealed partial class Column : DbColumn
	{
		public int? BaseColumnOrdinal { get; private set; }

		public DbType? CommonDataType { get; private set; }

		internal int? SeriesOrdinal { get; private set; }

		internal string? SeriesName { get; private set; }

		internal string? SeriesHeaderFormat { get; private set; }

		internal bool? IsSeries { get; private set; }

		internal Type? SeriesType { get; private set; }

		public string? Format { get; private set; }

		public override object? this[string property]
		{
			get
			{
				switch (property)
				{
					case nameof(CommonDataType):
						return this.CommonDataType;
					case nameof(SeriesOrdinal):
						return this.SeriesOrdinal;
					case nameof(SeriesName):
						return this.SeriesName;
					case nameof(SeriesHeaderFormat):
						return this.SeriesHeaderFormat;
					case nameof(IsSeries):
						return this.IsSeries;
					case nameof(SeriesType):
						return this.SeriesType;
					case nameof(Format):
						return this.Format;
					case nameof(BaseColumnOrdinal):
						return this.BaseColumnOrdinal;
					default:
						return base[property];
				}
			}
		}

		private Column() { }
	}
}
