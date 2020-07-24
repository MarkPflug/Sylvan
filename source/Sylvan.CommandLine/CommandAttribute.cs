using System;

namespace Sylvan.CommandLine
{
	public sealed class CommandAttribute : Attribute
	{
		public string? Name { get; }

		public CommandAttribute(string commandName)
		{
			this.Name = commandName;
		}

		public CommandAttribute()
		{
			this.Name = null;
		}
	}
}
