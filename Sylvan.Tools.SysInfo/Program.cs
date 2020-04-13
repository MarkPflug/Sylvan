using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.NetworkInformation;
using System.Runtime.InteropServices;

class Tool
{
	static bool IsColorEnabled = false;

	public static void Main()
	{
		try
		{
			var isLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
			var isOSX = RuntimeInformation.IsOSPlatform(OSPlatform.OSX);
			var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

			IsColorEnabled = TryEnableConsoleColor();

			ColorConsole.SetForeground(0x40, 0x80, 0x20);
			Value("Tool Version", typeof(Tool).Assembly.GetName().Version.ToString());

			Header("Machine");
			Value("Architecture", RuntimeInformation.ProcessArchitecture.ToString());
			Value("Runtime Version", RuntimeEnvironment.GetSystemVersion());

			Value("MachineName", Environment.MachineName);
			var os =
				isWindows
				? "Windows"
				: isLinux
				? "Linux"
				: isOSX
				? "OSX"
				: "Other";
			Value("OS", os);
			Value("OSVersion", Environment.OSVersion.ToString());
			Value("ProcessorCount", Environment.ProcessorCount.ToString());
			Value("SystemPageSize", Environment.SystemPageSize.ToString());
			var tickCount = Environment.TickCount;
			Value("SystemStarted", DateTime.Now.AddMilliseconds(-Environment.TickCount).ToString() + " (local)");
			Value("SystemUpTime", TimeSpan.FromMilliseconds(tickCount).ToString());

			Header("Special Folders");
			var specialFolders =
				Enum.GetNames(typeof(Environment.SpecialFolder))
					.Zip(Enum.GetValues(typeof(Environment.SpecialFolder)).Cast<Environment.SpecialFolder>(),
						 (name, value) => new
						 {
							 Name = name,
							 Path = Environment.GetFolderPath(value, Environment.SpecialFolderOption.DoNotVerify),
						 })
					.OrderBy(sf => sf.Name, StringComparer.OrdinalIgnoreCase)
					.ToArray();

			var maxSpecialFolderNameWith = specialFolders.Max(sf => sf.Name.Length);

			foreach (var specialFolder in specialFolders)
				Value(specialFolder.Name, specialFolder.Path, maxSpecialFolderNameWith);

			Header("Storage");
			var drives = System.IO.DriveInfo.GetDrives();

			bool first = true;
			foreach (var drive in drives)
			{
				if (first)
				{
					first = false;
				}
				else
				{
					Console.WriteLine();
				}


				Value("Name", drive.Name);
				Value("Type", drive.DriveType.ToString());
				Value("IsReady", drive.IsReady.ToString());

				if (drive.IsReady)
				{
					try
					{
						Value("Label", drive.VolumeLabel);
						Value("Format", drive.DriveFormat ?? "Unknown");
						Value("Size", FormatSize(drive.TotalSize));
						Value("Free", FormatSize(drive.AvailableFreeSpace));
					}
					catch
					{

					}
				}
			}

			Header("Time");
			Value("UTC Time", DateTime.UtcNow.ToString());
			Value("Local Time", DateTime.Now.ToString());
			Value("TimeZone", TimeZoneInfo.Local.StandardName);


			Header("Region/Culture");
			Value("Region", RegionInfo.CurrentRegion.Name);
			Value("Culture", CultureInfo.CurrentCulture.Name);
			Value("UICulture", CultureInfo.CurrentUICulture.Name);

			Header("User");
			Value("Domain", Environment.UserDomainName);
			Value("User", Environment.UserName);

			Header("Network");

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

					Value("Type", net.NetworkInterfaceType.ToString());
					Value("Description", net.Description);
					var props = net.GetIPProperties();
					foreach (var addr in props.UnicastAddresses)
					{
						Value(addr.Address.AddressFamily.ToString(), addr.Address.ToString());
					}
				}
			}

			Header("Environment");

			foreach (var kvp in Environment.GetEnvironmentVariables().Cast<DictionaryEntry>().OrderBy(kvp => kvp.Key))
			{
				var key = (string)kvp.Key;
				var value = (string)kvp.Value;

				if (StringComparer.OrdinalIgnoreCase.Equals(key, "LS_COLORS"))
				{
					LSColors(key, value);
				}
				else
				{
					if (SplitEnvVars.Contains(key))
					{
						var separator = isWindows ? ';' : ':';
						var values = value.Split(separator);
						Value(key, values, EnvVarWidth);
					}
					else
					{
						Value(key, value, EnvVarWidth);
					}
				}
			}
		}
		finally
		{
			// ensure restore the original settings on exit
			ColorConsole.SetDefaults();
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

	static void SetForegroundColor(int value)
	{
		if (IsColorEnabled)
		{

			var r = (byte)(value >> 16 & 0xff);
			var g = (byte)(value >> 8 & 0xff);
			var b = (byte)(value & 0xff);
			ColorConsole.SetForeground(r, g, b);
		}
	}

	static void SetLabelColor()
	{
		SetForegroundColor(LabelColor);
	}

	static void SetSeparatorColor()
	{
		SetForegroundColor(LabelColor);
	}

	static void SetValueColor()
	{
		SetForegroundColor(ValueColor);
	}

	static void SetHeaderColor()
	{
		SetForegroundColor(HeaderColor);
	}

	static void Label(string label)
	{
		SetLabelColor();
		Console.Write(label);
	}

	const int HeaderColor = 0x40a0f0;
	const int LabelColor = 0x60c060;
	const int SeparatorColor = 0xa0a0a0;
	const int ValueColor = 0xe0e0e0;

	static void Separator(string separator = ": ")
	{
		SetSeparatorColor();
		Console.Write(separator);
	}

	static void LSColors(string key, string value)
	{
		var items = value.Split(':');

		Console.Write(String.Format("{0," + EnvVarWidth + "}", key));

		Separator();

		ColorConsole.SetForeground(0xe0, 0xe0, 0xe0);

		bool first = true;
		foreach (var item in items)
		{
			if (first)
			{
				first = false;
			}
			else
			{
				Console.Write(new string(' ', EnvVarWidth + 2));
			}

			Console.Write($"{item,-20}");
			Console.Write(" ");
			var idx = item.IndexOf('=');
			if (idx > 0)
			{
				var itemKey = item.Substring(0, idx);
				var itemVal = item.Substring(idx + 1);
				var parts = itemVal.Split(';');
				foreach (var part in parts)
				{
					ColorConsole.SetCode(part);
				}

				Console.Write("(Color)");

				ColorConsole.SetDefaults();
				if (LSKeys.TryGetValue(itemKey, out string desc))
				{
					Console.Write(" ");
					Console.Write(desc);
				}
			}

			Console.WriteLine();
		}

		ColorConsole.SetDefaults();
	}

	const int HeaderWidth = 80;
	const int DefaultNameWidth = 16;
	const int EnvVarWidth = 32;
	const char HeaderChar = '-';
	const int PrePadCount = 3;
	static readonly string PrePad = new string(HeaderChar, PrePadCount);

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

	static bool TryEnableConsoleColor()
	{
		if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
		{
			return ColorConsole.EnableColorMode();
		}
		// other platforms should support VT escape sequences normally, I think... ?
		return true;
	}

	static void Header(string heading)
	{
		Console.WriteLine();
		SetHeaderColor();

		var l = HeaderWidth - 2 - PrePadCount - heading.Length;

		Console.Write(PrePad);
		Console.Write(' ');
		Console.Write(heading);
		Console.Write(' ');
		if (l > 0)
		{
			Console.Write(new string(HeaderChar, l));
		}
		Console.WriteLine();
		ColorConsole.SetDefaults();
	}

	static void Value(string name, string value, int width = DefaultNameWidth)
	{
		Value(name, new string[] { value }, width);
	}

	static void Value(string name, IEnumerable<string> values, int width = DefaultNameWidth)
	{
		Label(String.Format("{0," + width + "}", name));
		Separator();
		SetValueColor();

		bool first = true;
		foreach (var value in values)
		{
			if (first)
			{
				first = false;
			}
			else
			{
				Console.Write(new string(' ', width + 2));
			}
			Console.Write(value);
			Console.WriteLine();
		}

		ColorConsole.SetDefaults();
	}
}
