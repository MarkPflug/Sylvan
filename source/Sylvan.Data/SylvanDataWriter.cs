using System;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;

namespace Sylvan.Data
{

	public enum SylvanDataType
	{
		None = 0,
		Boolean = 1,
		Byte = 2,
		Int16 = 3,
		Int32 = 4,
		Int64 = 5,
		String = 6,
	}

	public class SylvanDataWriter
	{
		Stream stream;

		public SylvanDataWriter(Stream stream)
		{
			this.stream = stream;
		}
		const uint Marker = 0x766c7973;		
		const uint Version = 1;

		public async Task WriteAsync(DbDataReader data)
		{
			BinaryWriter w = new BinaryWriter(stream);
			w.Write(Marker);
			w.Write(Version);

			var fieldCount = data.FieldCount;
			w.Write(fieldCount);
			for (int i = 0; i < fieldCount; i++) {
				w.Write(data.GetName(i));
			}

			var x = 0;
			while (data.Read())
			{
				for (int i = 0; i < fieldCount; i++)
				{
					var t = data.GetFieldType(i);
					if (data.IsDBNull(i))
						continue;
					switch (Type.GetTypeCode(t))
					{
						case TypeCode.Boolean:
							w.Write(data.GetBoolean(i) ? (byte)1 : (byte)0);
							break;
						case TypeCode.Int32:
							w.Write(data.GetInt32(i));
							break;
						case TypeCode.String:
							w.Write(data.GetString(i));
							break;
						case TypeCode.Object:
						default:
							x++;
							w.Write(data.GetValue(i)?.ToString() ?? "");
							break;
					}					
				}
			}
		}
	}
}
