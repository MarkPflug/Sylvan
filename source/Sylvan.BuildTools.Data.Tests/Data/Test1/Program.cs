using System;
using System.Collections.Generic;
using System.Text;

class Program
{
	public static int Main()
	{
		var t = Type.GetType("Counties");
		Console.WriteLine("asdf");
		return t == null ? 0 : 1;
	}
}
