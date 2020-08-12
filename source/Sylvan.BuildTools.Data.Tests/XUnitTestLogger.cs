using Microsoft.Build.Framework;
using System.Collections;
using Xunit.Abstractions;

namespace Sylvan
{
	class XUnitTestLogger : ILogger
	{
		static bool DumpEnv = false;
		static bool Verbose = true;

		ITestOutputHelper o;

		public XUnitTestLogger(ITestOutputHelper o)
		{
			this.o = o;
		}

		public LoggerVerbosity Verbosity { get; set; }
		public string Parameters { get; set; }

		public void Initialize(IEventSource eventSource)
		{
			eventSource.AnyEventRaised += EventSource_AnyEventRaised;
			eventSource.BuildStarted += EventSource_BuildStarted;
			eventSource.ProjectStarted += EventSource_ProjectStarted;
			eventSource.MessageRaised += EventSource_MessageRaised;
			eventSource.ErrorRaised += EventSource_ErrorRaised;
			eventSource.WarningRaised += EventSource_WarningRaised;
			eventSource.TargetStarted += EventSource_TargetStarted;
			eventSource.TargetFinished += EventSource_TargetFinished;
		}

		private void EventSource_TargetFinished(object sender, TargetFinishedEventArgs e)
		{
			o.WriteLine("Target ending: " + e.TargetName);
		}

		private void EventSource_TargetStarted(object sender, TargetStartedEventArgs e)
		{
			o.WriteLine("Target starting: " + e.TargetName);
		}

		private void EventSource_WarningRaised(object sender, BuildWarningEventArgs e)
		{
			o.WriteLine(e.Message + " " + e.File + "(" + e.LineNumber + "," + e.ColumnNumber + ")");
		}

		private void EventSource_ErrorRaised(object sender, BuildErrorEventArgs e)
		{
			o.WriteLine(e.Message + " " + e.File + "(" + e.LineNumber + "," + e.ColumnNumber + ")");
		}

		private void EventSource_MessageRaised(object sender, BuildMessageEventArgs e)
		{
			o.WriteLine(e.Message);
		}

		private void EventSource_ProjectStarted(object sender, ProjectStartedEventArgs e)
		{
			if (DumpEnv)
			{
				o.WriteLine("Initial Properties:");
				foreach (DictionaryEntry kvp in e.Properties)
				{
					o.WriteLine($"{kvp.Key} {kvp.Value}");
				}
			}
		}

		private void EventSource_BuildStarted(object sender, BuildStartedEventArgs e)
		{
			if (DumpEnv)
			{
				o.WriteLine("Environment:");
				foreach (var kvp in e.BuildEnvironment)
				{
					o.WriteLine($"{kvp.Key}: {kvp.Value}");
				}
			}
		}

		private void EventSource_AnyEventRaised(object sender, BuildEventArgs e)
		{
			if (Verbose)
			{
				o.WriteLine(e.Message);
			}
		}

		public void Shutdown()
		{
		}
	}
}