using System;
using System.Collections.Generic;
using System.Text;

class Program
{
	public static int Main()
	{
		var t = Type.GetType("Issues");

		foreach(var issue in IssuesSet.Read())
        {
			Console.WriteLine(issue.State);
			Console.WriteLine(issue.County);
			foreach(var v in issue.Values)
            {
				Console.WriteLine(v);
			}
		}
		
		return t == null ? 0 : 1;
	}
}
