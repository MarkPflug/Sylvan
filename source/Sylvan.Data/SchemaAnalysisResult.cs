﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Text.RegularExpressions;

namespace Sylvan.Data
{
	public class AnalysisResult : IEnumerable<ColumnInfo>
	{
		readonly ColumnInfo[] columns;

		internal AnalysisResult(ColumnInfo[] columns)
		{
			this.columns = columns;
		}

		public IEnumerator<ColumnInfo> GetEnumerator()
		{
			foreach (var col in columns)
				yield return col;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.GetEnumerator();
		}

		public ReadOnlyCollection<DbColumn> GetSchema()
		{
			var series = DetectSeries(columns);
			var schema = new List<DbColumn>();
			for (int i = 0; i < columns.Length; i++)
			{
				var col = columns[i];

				if (series?.seriesStart == i)
				{
					int idx = i;
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
					var columnSchema = Schema.SchemaColumn.CreateSeries("Values", allowNull, type, series.type, prefix + "{" + series.type + "}");
					i = series.seriesEnd;
					schema.Add(columnSchema);
					continue;
				}
				var dbCol = col.CreateColumnSchema();
				schema.Add(dbCol);
			}
			return new ReadOnlyCollection<DbColumn>(schema);
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

				if (DateTime.TryParse(name, out var _))
				{
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
}
