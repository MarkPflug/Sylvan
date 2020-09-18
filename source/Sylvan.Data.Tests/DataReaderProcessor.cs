using System;
using System.Data;

namespace Sylvan.Data
{
	public static class DataReaderProcessor
	{
		public static void Process(this IDataReader reader)
		{
			while (reader.Read())
			{
				for(int i = 0; i < reader.FieldCount; i++)
				{
					if (reader.IsDBNull(i))
						continue;

					switch (Type.GetTypeCode(reader.GetFieldType(i)))
					{
						case TypeCode.Boolean:
							reader.GetBoolean(i);
							break;
						case TypeCode.Int32:
							reader.GetInt32(i);
							break;
						case TypeCode.DateTime:
							reader.GetDateTime(i);
							break;
						case TypeCode.Double:
							reader.GetDouble(i);
							break;
						case TypeCode.Decimal:
							reader.GetDecimal(i);
							break;
						default:
							continue;							
					}
				}
			}
		}
	}
}
