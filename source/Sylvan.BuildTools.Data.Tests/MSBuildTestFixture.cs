using Microsoft.Build.Locator;
using System;
using System.IO;
using System.Reflection;
using Xunit;

namespace Sylvan.BuildTools.Data.Tests
{
	[CollectionDefinition("MSBuild")]
	public class MSBuildCollection : ICollectionFixture<MsBuildFixture> { }

	class MsBuildFixture : IDisposable
	{
		public MsBuildFixture()
		{
			var vs = MSBuildLocator.RegisterDefaults();
			AppDomain.CurrentDomain.AssemblyResolve += CurrentDomain_AssemblyResolve;
			AppDomain.CurrentDomain.AssemblyLoad += CurrentDomain_AssemblyLoad;

		}

		private void CurrentDomain_AssemblyLoad(object sender, AssemblyLoadEventArgs args)
		{
			if (args.LoadedAssembly.GetName().Name == "Microsoft.Build.Framework")
			{
				this.sdkPath = System.IO.Path.GetDirectoryName(args.LoadedAssembly.Location);
			}
		}

		string sdkPath;

		private System.Reflection.Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
		{
			if (args.Name != null && sdkPath != null)
			{
				var name = new AssemblyName(args.Name).Name;
				var file = Path.Combine(sdkPath, name + ".dll");
				if (File.Exists(file))
				{
					return Assembly.LoadFrom(file);
				}
			}
			return null;
		}

		public void Dispose()
		{
			MSBuildLocator.Unregister();
		}
	}
}
