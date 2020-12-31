using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Sylvan.Data.XBase.Tests
{
	public class EncodingTests
	{
		[Fact]
		public void VerifyEncodingSupport()
		{

#if NET5_0
			// encodings are available by default on net461
			Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
#endif

			int[] s =
				new int[] {
					437,
					620 ,
					737 ,
					850 ,
					852 ,
					857 ,
					861 ,
					865 ,
					866 ,
					874 ,
					895 ,
					932 ,
					936 ,
					949 ,
					950 ,
					1250 ,
					1251 ,
					1252 ,
					1253 ,
					1254 ,
					1255 ,
					1256 ,
					10000 ,
					10006 ,
					10007 ,
					10029
			};

			HashSet<int> supported = new HashSet<int>();
			HashSet<int> unsupported = new HashSet<int>();

			foreach (var c in s)
			{
				try
				{
					var enc = Encoding.GetEncoding(c);
					supported.Add(c);
				}
				catch (Exception)
				{
					unsupported.Add(c);
				}
			}
		}
	}
}