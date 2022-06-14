using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Sylvan.Data;

/// <summary>
/// The result of a data analysis process.
/// </summary>
public class AnalysisResult : IEnumerable<ColumnInfo>
{
	readonly ColumnInfo[] columns;
	readonly bool detectSeries;

	internal AnalysisResult(bool detectSeries, ColumnInfo[] columns)
	{
		this.detectSeries = detectSeries;
		this.columns = columns;
	}

	/// <summary>
	/// Enumerates the columns in the analysis result.
	/// </summary>
	/// <returns></returns>
	public IEnumerator<ColumnInfo> GetEnumerator()
	{
		foreach (var col in columns)
			yield return col;
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return this.GetEnumerator();
	}

	/// <summary>
	/// Gets a schema representing the analysis result.
	/// </summary>
	/// <returns></returns>
	public Schema GetSchema()
	{
		return GetSchemaBuilder().Build();
	}

	/// <summary>
	/// Gets the schema builder for the analysis result.
	/// </summary>
	public Schema.Builder GetSchemaBuilder()
	{
		var series = detectSeries ? DetectSeries(columns) : null;
		var schema = new Schema.Builder();

		for (int i = 0; i < columns.Length; i++)
		{
			var col = columns[i];

			if (series?.seriesStart == i)
			{
				string? prefix = series.prefix;
				var types = col.GetColType();
				var allowNull = false;
				for (; i <= series.seriesEnd; i++)
				{
					col = columns[i];
					allowNull |= col.AllowDbNull;
					types &= col.GetColType();
				}
				var type = ColumnInfo.GetType(types);

				var name = string.IsNullOrEmpty(series.prefix) ? "Values" : series.prefix;
				var cb = new Schema.Column.Builder(name + "*", type, allowNull)
				{
					IsSeries = true,
					SeriesName = name,
					SeriesOrdinal = 0,
					SeriesType = series.type == SeriesType.Integer ? typeof(int) : typeof(DateTime),
					SeriesHeaderFormat = prefix + "{" + series.type + "}",
				};

				i = series.seriesEnd;
				schema.Add(cb);
				continue;
			}
			var dbCol = col.CreateColumnSchema();
			schema.Add(dbCol);
		}
		return schema;
	}

	class SeriesInfo
	{
		public SeriesInfo(int idx)
		{
			this.seriesStart = idx;
			this.seriesEnd = idx;
		}

		public SeriesType type;
		public string? prefix;
		public int value = -1;
		public int step;
		public int seriesStart;
		public int seriesEnd;

		public int Length => seriesEnd - seriesStart + 1;
	}

	static string? GetDateSeriesPrefix(string name)
	{
		for (int i = 0; i < name.Length - 4; i++)
		{
			if (DateTime.TryParse(name.Substring(i), out DateTime value))
			{
				return name.Substring(0, i);
			}
		}
		return null;
	}

	SeriesInfo? DetectSeries(ColumnInfo[] cols)
	{
		var series = new SeriesInfo[cols.Length];

		SeriesInfo? ss = null;

		for (int i = 0; i < this.columns.Length; i++)
		{
			var s = series[i] = new SeriesInfo(i);

			var col = this.columns[i];
			var name = col.Name;
			if (name == null) continue;

			var dateSeriesPrefix = GetDateSeriesPrefix(name);

			if (dateSeriesPrefix != null)
			{
				s.prefix = dateSeriesPrefix;
				s.type |= SeriesType.Date;
			}
			else
			{
				var match = Regex.Match(name, @"\d+$");
				if (match.Success)
				{
					var prefix = name.Substring(0, name.Length - match.Length);
					s.prefix = prefix;
					s.value = int.Parse(match.Captures[0].Value);
					s.type |= SeriesType.Integer;
				}
			}

			if (i > 0 && s.type != SeriesType.None)
			{
				var prev = series[i - 1];
				var start = series[prev.seriesStart];
				var step = s.value - prev.value;
				if (prev.type == s.type && StringComparer.InvariantCultureIgnoreCase.Equals(prev.prefix, s.prefix))
				{
					s.seriesStart = prev.seriesStart;
					ss = ss == null ? start : ss != start && start.Length > ss.Length ? start : ss;
					s.step = step;
					series[s.seriesStart].seriesEnd = i;
				}
			}
		}
		return ss;
	}
}
