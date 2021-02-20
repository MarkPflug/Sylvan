using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Sylvan.Tools
{
	static class Drive
	{
		[DllImport("Kernel32", ExactSpelling = true, EntryPoint = "GetDiskFreeSpaceW", SetLastError = true, CharSet = CharSet.Unicode)]
		static extern bool GetDiskFreeSpace(
			string lpRootPathName, 
			out uint lpSectorsPerCluster, 
			out uint lpBytesPerSector, 
			out uint lpNumberOfFreeClusters, 
			out uint lpTotalNumberOfClusters);

		[MethodImpl(MethodImplOptions.NoInlining)]
		static int GetClusterSizeWindows(string driveName)
		{
			uint a, b, c, d;
			if (GetDiskFreeSpace(driveName, out a, out b, out c, out d))
			{
				return (int) (a * b);
			}
			return -1;
		}

		public static int GetClusterSize(this DriveInfo drive)
		{
			if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
			{
				return GetClusterSizeWindows(drive.Name);
			}
			// TODO: other platforms.
			return -1;
		}
	}
}
