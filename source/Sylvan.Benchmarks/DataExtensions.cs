using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Sylvan.Benchmarks;

static class DataExtensions
{
	public static void ProcessData(this DbDataReader dr)
	{
		var types = new TypeCode[dr.FieldCount];

		for (int i = 0; i < types.Length; i++)
		{
			types[i] = Type.GetTypeCode(dr.GetFieldType(i));
		}
		while (dr.Read())
		{
			for (int i = 0; i < dr.FieldCount; i++)
			{
				switch (types[i])
				{
					case TypeCode.Boolean:
						var b = dr.GetBoolean(i);
						break;
					case TypeCode.DateTime:
						var dt = dr.GetDateTime(i);
						break;
					case TypeCode.Int32:
						var v = dr.GetInt32(i);
						break;
					case TypeCode.Double:
						var d = dr.GetDouble(i);
						break;
					case TypeCode.String:
						var s = dr.GetString(i);
						break;
					case TypeCode.Object:
						if (dr.GetFieldType(i) == typeof(Guid))
						{
							var g = dr.GetGuid(i);
							break;
						}
						goto default;
					default:
						throw new NotSupportedException();
				}
			}
		}
	}

	public static async Task ProcessDataAsync(this DbDataReader dr)
	{
		var types = new TypeCode[dr.FieldCount];

		for (int i = 0; i < types.Length; i++)
		{
			types[i] = Type.GetTypeCode(dr.GetFieldType(i));
		}
		while (await dr.ReadAsync())
		{
			for (int i = 0; i < dr.FieldCount; i++)
			{
				switch (types[i])
				{
					case TypeCode.Boolean:
						var b = dr.GetBoolean(i);
						break;
					case TypeCode.DateTime:
						var dt = dr.GetDateTime(i);
						break;
					case TypeCode.Int32:
						var v = dr.GetInt32(i);
						break;
					case TypeCode.Double:				
						var d = dr.GetDouble(i);
						break;
					case TypeCode.String:
						var s = dr.GetString(i);
						break;
					case TypeCode.Object:
						if(dr.GetFieldType(i) == typeof(Guid))
						{
							var g = dr.GetGuid(i);
							break;
						}
					goto default;
					default:
						throw new NotSupportedException();
				}
			}
		}
	}

	public static async Task ProcessDataGenericAsync(this DbDataReader dr)
	{
		var types = new TypeCode[dr.FieldCount];

		for (int i = 0; i < types.Length; i++)
		{
			types[i] = Type.GetTypeCode(dr.GetFieldType(i));
		}
		while (await dr.ReadAsync())
		{
			for (int i = 0; i < dr.FieldCount; i++)
			{
				switch (types[i])
				{
					case TypeCode.Int32:
						var v = dr.GetFieldValue<int>(i);
						break;
					case TypeCode.Double:
						var d = dr.GetFieldValue<double>(i);
						break;
					case TypeCode.String:
						var s = dr.GetFieldValue<string>(i);
						break;
					default:
						throw new NotSupportedException();
				}
			}
		}
	}
}
