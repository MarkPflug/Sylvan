using Microsoft.Build.Evaluation;
using Microsoft.Build.Framework;
using Microsoft.Build.Locator;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using Xunit;
using Xunit.Abstractions;

namespace Sylvan.BuildTools.Data.Tests
{
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
			if(args.LoadedAssembly.GetName().Name == "Microsoft.Build.Framework")
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

	[CollectionDefinition("MSBuild")]
	public class MSBuildCollection : ICollectionFixture<MsBuildFixture>
	{
	}

	[Collection("MSBuild")]
	public class SylvanDataCodeGenTests
	{
		ILogger logger;
		ITestOutputHelper o;

		static Dictionary<string, string> gp =
			new Dictionary<string, string>
			{
				["Configuration"] = "Debug",
				["Platform"] = "AnyCPU",
			};

		public SylvanDataCodeGenTests(ITestOutputHelper o)
		{
			this.o = o;
			this.logger = new XUnitTestLogger(o);
		}

		(int, string) GetOutput(string exePath, string args = "")
		{
			var psi = new ProcessStartInfo()
			{
				FileName = "dotnet",
				Arguments = exePath + " " + args,
				UseShellExecute = false,
				RedirectStandardOutput = true,
				CreateNoWindow = true,
			};
			var proc = Process.Start(psi);
			var text = proc.StandardOutput.ReadToEnd();
			proc.WaitForExit();
			var exitCode = proc.ExitCode;
			return (exitCode, text);
		}

		void LogProps(Project proj)
		{
			foreach (var kvp in Environment.GetEnvironmentVariables().Cast<DictionaryEntry>().OrderBy(e => e.Key))
			{
				o.WriteLine(kvp.Key + ": " + kvp.Value);
			}
			foreach (var prop in proj.AllEvaluatedProperties.OrderBy(p => p.Name))
			{
				o.WriteLine(prop.Name + ": " + prop.EvaluatedValue + " (" + prop.UnevaluatedValue + ")");
			}
		}

		string BuildProject(string projFile)
		{
			var pc = new ProjectCollection(gp);
			var proj = pc.LoadProject(projFile);
			var success = proj.Build("Restore", new[] { logger });
			//if (!success) LogProps(proj);
			Assert.True(success, "Failed to restore packages");
			success = proj.Build(logger);
			if (!success) LogProps(proj);

			var outputPath = proj.GetPropertyValue("TargetPath");
			Assert.True(success, "Build failed");
			return outputPath;
		}

		[Fact]
		public void BuildTest()
		{
			var exepath = BuildProject("Data/Test1/Test1.csproj");
			var (exitcode, output) = GetOutput(exepath);
			Assert.Equal(0, exitcode);
		}
	}
}
