using System.Data.Common;

namespace Sylvan.Data.Csv
{
	class BoundedDataReader : DataReaderAdapter
	{
		int rows;
		int count = 0;

		public BoundedDataReader(DbDataReader dr, int rows) : base(dr)
		{
			this.rows = rows;
		}		

		public override bool Read()
		{
			var success = count < rows && Reader.Read();

			if (success)
			{
				count++;
			}
			return success;
		}
	}
}
