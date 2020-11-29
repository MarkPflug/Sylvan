using Microsoft.Build.Evaluation;
using Microsoft.Build.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Sylvan.BuildTools.Data
{
	public abstract class MSBuildTest
	{
		ILogger logger;
		ITestOutputHelper o;

		static Dictionary<string, string> gp =
			new Dictionary<string, string>
			{
				["Configuration"] = "Debug",
				["Platform"] = "AnyCPU",
			};

		public MSBuildTest(ITestOutputHelper o)
		{
			this.o = o;
			this.logger = new XUnitTestLogger(o);
		}

		protected (int, string, string) GetOutput(string exePath, string args = "")
		{
			var psi = new ProcessStartInfo()
			{
				FileName = "dotnet",
				Arguments = exePath + " " + args,
				WorkingDirectory = Path.GetDirectoryName(exePath),
				UseShellExecute = false,
				RedirectStandardOutput = true,
				RedirectStandardError = true,
				CreateNoWindow = true,
			};
			var proc = Process.Start(psi);
			var text = proc.StandardOutput.ReadToEnd();
			var err = proc.StandardError.ReadToEnd();
			proc.WaitForExit();
			var exitCode = proc.ExitCode;
			return (exitCode, text, err);
		}

		protected void LogProps(Project proj)
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

		protected string BuildProject(string projFile, bool restore = true)
		{
			var pc = new ProjectCollection(gp);
			var proj = pc.LoadProject(projFile);
			bool success = false;
			if (restore)
			{
				success = proj.Build("Restore", new[] { logger });
				if (!success) LogProps(proj);
				Assert.True(success, "Failed to restore packages");
			}
			success = proj.Build(logger);
			//if (!success) LogProps(proj);

			var outputPath = proj.GetPropertyValue("TargetPath");
			Assert.True(success, "Build failed");
			return outputPath;
		}

		protected string BuildProjectNoRestore(string projFile)
		{
			var pc = new ProjectCollection(gp);
			var proj = pc.LoadProject(projFile);
			
			var success = proj.Build(logger);
			//if (!success) LogProps(proj);

			var outputPath = proj.GetPropertyValue("TargetPath");
			Assert.True(success, "Build failed");
			return outputPath;
		}
	}
}
