using System;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.IO;

namespace Sylvan.Data
{

	public class DataSchema : IDbColumnSchemaGenerator
	{
		//public string Name { get; }

		public ReadOnlyCollection<DbColumn> GetColumnSchema()
		{
			throw new NotImplementedException();
		}

		public static void Write(TextWriter writer, ReadOnlyCollection<DbColumn> schema)
		{
			foreach (var col in schema)
			{
				if (col.BaseColumnName != null && col.BaseColumnName != col.ColumnName) {
					writer.Write(col.BaseColumnName);
					writer.Write("=>");
				}
				writer.Write(col.ColumnName);
				var type = GetTypeName(col);
				writer.Write(' ');
				writer.Write(type);
				if (col.AllowDBNull == true)
				{
					writer.Write('?');
				}
				if (col.IsUnique == true)
				{
					writer.Write('!');
				}
				if (col.IsAutoIncrement == true)
				{
					writer.Write("#");
				}

				writer.WriteLine();
			}
		}

		static string GetTypeName(DbColumn col)
		{
			var type = col.DataType;
			switch (Type.GetTypeCode(type))
			{
				case TypeCode.Boolean:
					return "boolean";
				case TypeCode.Char:
					return "char";
				case TypeCode.Int16:
					return "int16";
				case TypeCode.Int32:
					return "int32";
				case TypeCode.Int64:
					return "int64";
				case TypeCode.Single:
					return "single";
				case TypeCode.Double:
					return "double";
				case TypeCode.String:
					if (col.IsLong == true)
					{
						return "text";
					}
					else
					{
						return "string";
					}
				case TypeCode.DateTime:
					return "datetime";
				case TypeCode.Object:
					if(type == typeof(byte[]))
					{
						if(col.IsLong == true)
						{
							return "blob";
						}
						else
						{
							var tn = "binary";
							if(col.ColumnSize != null)
							{
								tn += "[" + col.ColumnSize + "]";
							}
							return tn;
						}						
					}
					else if(type == typeof(Guid))
					{
						return "guid";
					}
					goto default;
				default:
					return "object";
			}
		}
	}
}
