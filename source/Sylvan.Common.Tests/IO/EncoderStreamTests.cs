using Sylvan.Benchmarks;
using System;
using System.IO;
using System.Text;
using Xunit;

namespace Sylvan.IO
{
	public class EncoderStreamTests
	{
		byte[] inputData;

		public EncoderStreamTests()
		{
			var sw = new StringWriter();
			for (int i = 0; i < 100; i++)
			{
				sw.WriteLine($"{i}: abcdefghijklmnopqrstuvwxyz.");
			}
			inputData = Encoding.ASCII.GetBytes(sw.ToString());
		}

		[Fact]
		public void Test1()
		{
			var ms = new MemoryStream();
			var enc = new Base64Encoder();
			var s = new EncoderStream(ms, enc);
			s.Write(inputData, 0, inputData.Length);
			s.Flush();
			s.Close();
			ms.Position = 0;
			var str = Encoding.ASCII.GetString(ms.GetBuffer(), 0, (int) ms.Length);
			var datar = Convert.FromBase64String(str);
			var debug = Encoding.ASCII.GetString(datar);

			Assert.Equal(inputData, datar);
		}

		[Fact]
		public void MimeKitTest()
		{
			new Base64Benchmarks().MimeKitEnc();
		}
	}
}
/*
 * 
OI5f7Znp+edKpklVOyWNF/AqJ7uQPIV3eURhttkmnjtamzOew8scoK7n1hb9Eov4y/gzSuFIzvsI
6HNUh0INJAcO+5/x71QkDYYMqWVveWe5aerh8lRZEcw3ldtYyBhs2bEO7xfrB9ykDz1bOyWWmW4L
mh5YLDKztOnSuVp4y0RtoNCzYIpQzmEOPOMX/dWnofxsTC35E61/WzotGE2CK0P47ZtV4jjku8y8
SvBOm7YTKxhdDH8Wc4wiMwVQIrUUjBCfHhA6Ur7Eh5BhPMm9Mb+DGu00JB/E4nkPMq4ylJkXqL7f
rn2m2dhZmYTutTbCZQJFzAjH32fx2zyWKiAQ8/dJlAvdgkHcQBXbiprWLEJMzavsx5FPyFbu00EQ
rrhJCJD3a6K2h4AD78eyBN1XBQ9T3lNkpCxm+sy1768AzJFdkYIJ3sGglUUr6NfJnMN5l80yvNBk
5WNkkXDWAZjRAyM2VXNaTSZNskgNOLtUnF4IPFjhzg/A+KwfpE1QypSsWuW0kkZPHkmn5AmIV6yn
D/QSzcRA7zUTdV3HO6nY/Oi0YN7WoAkXuB0OOekFOKH/d5RZPv3PbGqwoB3oiOKgz+ch1PbN0W/q
+hSmMbZLDTy8x8QUfr7r5PRzJf/sLjA7bmoC5GSqMaUlb0ToKDu0ZZPgLMAejFApNu8CWqxvy+Rr
w0tIKNgRlR7uD3oCu7m/wGwLAm8c4hnZrnSo1/m1cHgEvHdoHGoIGiv/nUCuMzNJmusD8sl0T+Ex
b3pjab0lOJ4B/v9BVqMCAudEHETYKnpBXz0Nsn2vj22WN6zvpbFt2mSirTU0B0RHAfDhMDKcWEX/
IgmAF4UgUm6Sqh/3El0210LfNn1FDFf4xrGNzl86+t5n74Oo7MPegVrHol0lUO6Tq1nwvCLIuhWx
iFHUk4QPV7BBkCLZjut/AbUifWr603XC+iMGSj67nzRt9mNxCu3WuEqt5MDMuRxtcLtspT2rt7Tb
EZlhCA1yhfHtbJeHDfbHTMUfTO+19UAG2YJP+ffCPq+W+FR0rz07Xs/fTYXmY8SlWHeMasUmHFhb
cDPvNvf1qVjXdv5y1xbhEZDmFvOAsXW9qaCANtTpcNz8GT8KTCjX6aMsDuzGM4Fhqlk1PwVl2r69
MYxWdXOE7gYhdr7X18VnDi1PVKKJ93YfH4t7tDDXlz+ATBqcgXQvqCDmpLzCitOTzTHYFYYbNDkW
NlboCTrWSt5/Vouqy4y4IKyCo1K5XZS45+b+/oG5ZBfghkseR57mEFndQzNI6AKlH/XZmZHnXNnB
MCFT10xdxvJzykVBmtgNnJP52xlhb/7jWd6M
*/