using System;
using System.Data.Common;

namespace Sylvan.Data;

sealed class VariableDataReader<T> : DataReaderAdapter where T : DbDataReader
{
	readonly T dr;
	readonly Func<T, int> rowFieldCountSelector;
	readonly Type extraFieldType;

	public VariableDataReader(T dr, Func<T, int> rowFieldCountSelector, Type extraFieldType) : base(dr)
	{
		this.dr = dr;
		this.rowFieldCountSelector = rowFieldCountSelector;
		this.extraFieldType = extraFieldType;
	}

	public override Type GetFieldType(int ordinal)
	{
		return ordinal < dr.FieldCount
			? dr.GetFieldType(ordinal)
			: extraFieldType;
	}

	public override int FieldCount
	{
		get
		{
			return this.rowFieldCountSelector(this.dr);
		}
	}
}
