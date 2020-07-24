using System;

namespace Sylvan.CommandLine
{
	[AttributeUsage(AttributeTargets.Parameter, AllowMultiple = true, Inherited = false)]
	public class ParameterNameAttribute : Attribute
	{
	}
}
