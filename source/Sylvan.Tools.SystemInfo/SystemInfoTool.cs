using Sylvan.Terminal;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Runtime.InteropServices;

namespace Sylvan.Tools
{
	class SystemInfoTool
	{
		const string DateFormat = "yyyy-MM-dd HH:mm:ss";

		public static void Main()
		{
			using var trm = new ColorConsole();
			var iw = new InfoWriter(trm);

			var isLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
			var isOSX = RuntimeInformation.IsOSPlatform(OSPlatform.OSX);
			var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

			iw.Value("Tool Version", typeof(SystemInfoTool).Assembly.GetName().Version.ToString());

			iw.Header("Machine");
			iw.Value("Architecture", RuntimeInformation.ProcessArchitecture.ToString());
			iw.Value("Runtime Version", RuntimeEnvironment.GetSystemVersion());

			iw.Value("MachineName", Environment.MachineName);
			var os =
				isWindows
				? "Windows"
				: isLinux
				? "Linux"
				: isOSX
				? "OSX"
				: "Other";

			iw.Value("OS", os);
			iw.Value("OSVersion", Environment.OSVersion.ToString());
			iw.Value("ProcessorCount", Environment.ProcessorCount.ToString());
			iw.Value("SystemPageSize", Environment.SystemPageSize.ToString());
			iw.Value("OSPlatform", Environment.Is64BitOperatingSystem ? "64" : "32");

			var tickCount = Environment.TickCount64;
			iw.Value("SystemStarted", DateTime.Now.AddMilliseconds(-tickCount).ToString(DateFormat) + " (local)");
			iw.Value("SystemUpTime", TimeSpan.FromMilliseconds(tickCount).ToString(@"d\.hh\:mm\:ss\.fff"));

			iw.Header("Memory");
			var mi = Memory.GetMemoryInfo();
			if (mi != null)
			{
				iw.Value("Total", FormatSize(mi.Total));
				iw.Value("Available", FormatSize(mi.Available));
			}
			else
			{
				iw.Value("Total", "unknown");
				iw.Value("Available", "unknown");
			}


			iw.Header("Storage");
			var drives = DriveInfo.GetDrives();

			bool first = true;
			foreach (var drive in drives.Where(d => d.DriveType == DriveType.Fixed))
			{
				if (first)
				{
					first = false;
				}
				else
				{
					Console.WriteLine();
				}

				iw.Value("Name", drive.Name);
				iw.Value("Type", drive.DriveType.ToString());
				iw.Value("IsReady", drive.IsReady.ToString());

				if (drive.IsReady)
				{
					iw.Value("Label", drive.VolumeLabel);
					iw.Value("Format", drive.DriveFormat ?? "Unknown");
					iw.Value("Size", FormatSize(drive.TotalSize));
					iw.Value("Free", FormatSize(drive.AvailableFreeSpace));
					var clusterSize = drive.GetClusterSize();
					if (clusterSize > 0)
					{
						iw.Value("Allocation", clusterSize.ToString());
					}
				}
			}

			iw.Header("Special Folders");
			var specialFolders =
				Enum.GetNames(typeof(Environment.SpecialFolder))
					.Zip(Enum.GetValues(typeof(Environment.SpecialFolder)).Cast<Environment.SpecialFolder>(),
							(name, value) => new
							{
								Name = name,
								Path = Environment.GetFolderPath(value, Environment.SpecialFolderOption.DoNotVerify),
							})
					.Where(sf => !string.IsNullOrEmpty(sf.Path))
					.OrderBy(sf => sf.Name, StringComparer.OrdinalIgnoreCase)
					.ToArray();

			var maxSpecialFolderNameWith = specialFolders.Max(sf => sf.Name.Length) + 1;

			foreach (var specialFolder in specialFolders)
				iw.Value(specialFolder.Name, specialFolder.Path, maxSpecialFolderNameWith);

			iw.Header("Time");
			iw.Value("UTC Time", DateTime.UtcNow.ToString(DateFormat) + " (UTC)");
			iw.Value("Local Time", DateTime.Now.ToString(DateFormat) + " (local)");
			iw.Value("TimeZone", TimeZoneInfo.Local.StandardName);


			iw.Header("Region/Culture");
			iw.Value("Region", RegionInfo.CurrentRegion.Name);
			iw.Value("Culture", CultureInfo.CurrentCulture.Name);
			iw.Value("UICulture", CultureInfo.CurrentUICulture.Name);

			iw.Header("User");
			iw.Value("Domain", Environment.UserDomainName);
			iw.Value("User", Environment.UserName);

			iw.Header("Network");

			first = true;
			foreach (var net in NetworkInterface.GetAllNetworkInterfaces())
			{
				if (net.OperationalStatus == OperationalStatus.Up && net.NetworkInterfaceType != NetworkInterfaceType.Loopback)
				{
					if (first)
					{
						first = false;
					}
					else
					{
						Console.WriteLine();
					}

					iw.Value("Type", net.NetworkInterfaceType.ToString());
					iw.Value("Description", net.Description);
					var props = net.GetIPProperties();
					foreach (var addr in props.UnicastAddresses)
					{
						iw.Value(addr.Address.AddressFamily.ToString(), addr.Address.ToString());
					}
				}
			}

			iw.Header("Environment");

			foreach (var kvp in Environment.GetEnvironmentVariables().Cast<DictionaryEntry>().OrderBy(kvp => kvp.Key))
			{
				var key = (string)kvp.Key;
				var value = (string)kvp.Value;

				if (StringComparer.OrdinalIgnoreCase.Equals(key, "LS_COLORS"))
				{
					LSColors(trm, iw, key, value);
				}
				else
				{
					if (SplitEnvVars.Contains(key))
					{
						var separator = isWindows ? ';' : ':';
						var values = value.Split(separator);
						iw.Value(key, values, EnvVarWidth);
					}
					else
					{
						iw.Value(key, value, EnvVarWidth);
					}
				}
			}
		}

		const long KB = 1024;
		const long MB = KB * KB;
		const long GB = MB * KB;
		const long TB = GB * KB;

		static string FormatSize(long size)
		{
			if (size > TB)
			{
				return (size / (float)TB).ToString("0.00") + "TB";
			}

			if (size > GB)
			{
				return (size / (float)GB).ToString("0.00") + "GB";
			}

			if (size > MB)
			{
				return (size / (float)MB).ToString("0.00") + "MB";
			}

			return size + "b";
		}


		const int EnvVarWidth = 32;

		// environment variables to split.
		static HashSet<string> SplitEnvVars =
			new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
			"Path",
			"PSModulePath",
			"PathEXT"
			};

		static Dictionary<string, string> LSKeys = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
		{
			// from http://www.bigsoft.co.uk/blog/2008/04/11/configuring-ls_colors
			["no"] = "normal",
			["fi"] = "file",
			["di"] = "directory",
			["ln"] = "symlink",
			["pi"] = "pipe",
			["do"] = "door",
			["bd"] = "block device",
			["cd"] = "character device",
			["or"] = "orphan",
			["so"] = "socket",
			["su"] = "setuid",
			["sg"] = "setgid",
			["tw"] = "sticky other writable",
			["ow"] = "other writable",
			["st"] = "sticky",
			["ex"] = "executable",
			["mi"] = "missing",
			["lc"] = "left code",
			["rc"] = "right code",
			["ec"] = "end code",
		};

		static void LSColors(ColorConsole trm, InfoWriter iw, string key, string value)
		{
			var items = value.Split(':');

			trm.Write(string.Format("{0," + EnvVarWidth + "}", key));

			iw.Separator();

			trm.SetForeground(0xe0, 0xe0, 0xe0);

			bool first = true;
			foreach (var item in items)
			{
				if (first)
				{
					first = false;
				}
				else
				{
					trm.Write(new string(' ', EnvVarWidth + 2));
				}

				trm.Write($"{item,-20}");
				trm.Write(" ");
				var idx = item.IndexOf('=');
				if (idx > 0)
				{
					var itemKey = item.Substring(0, idx);
					var itemVal = item.Substring(idx + 1);
					var parts = itemVal.Split(';');
					foreach (var part in parts)
					{
						trm.SetCode(part);
					}

					trm.Write("(Color)");

					trm.SetDefaults();
					if (LSKeys.TryGetValue(itemKey, out string desc))
					{
						trm.Write(" ");
						trm.Write(desc);
					}
				}

				trm.WriteLine();
			}

			trm.SetDefaults();
		}
	}
}