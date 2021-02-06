using System;
using System.IO;

namespace Sylvan.IO
{
	/// <summary>
	/// Extension methods for Stream.
	/// </summary>
	public static class StreamExtensions
	{
		/// <summary>
		/// Creates a stream that will report read progress.
		/// </summary>
		/// <param name="stream">A stream that is readable and seekable, usually a file stream.</param>
		/// <param name="callback">The callback to be notified of progress.</param>
		/// <param name="factor">A value indicating the frequency of progress updates, between 0.0 and 1.0 exclusive.</param>
		/// <returns>A stream that will report progress when read.</returns>
		public static Stream WithReadProgress(this Stream stream, Action<double> callback, double factor = 0.01)
		{
			if (stream == null) throw new ArgumentNullException(nameof(stream));
			if (callback == null) throw new ArgumentNullException(nameof(callback));

			if (stream.CanRead == false)
			{
				throw new ArgumentException("Stream must be readable", nameof(stream));
			}
			if (stream.CanSeek == false)
			{
				throw new ArgumentException("Stream must be seekable", nameof(stream));
			}
			if (factor <= 0d || factor >= 1d)
				throw new ArgumentOutOfRangeException(nameof(factor));
			if (callback == null)
				throw new ArgumentNullException(nameof(callback));

			return new ProgressStream(stream, callback, factor);
		}
	}
}