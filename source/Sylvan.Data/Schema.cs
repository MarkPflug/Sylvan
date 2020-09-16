﻿using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;

namespace Sylvan.Data
{
	/// <summary>
	/// Provides schema information for data.
	/// </summary>
	public sealed class Schema : IDbColumnSchemaGenerator
	{
		static readonly Lazy<Dictionary<string, DbType>> ColumnTypeMap = new Lazy<Dictionary<string, DbType>>(InitializeTypeMap);

		static Dictionary<string, DbType> InitializeTypeMap()
		{
			var map = new Dictionary<string, DbType>(StringComparer.OrdinalIgnoreCase);
			var values = Enum.GetValues(typeof(DbType));
			foreach (DbType type in values)
			{
				map.Add(type.ToString(), type);
			}
			map.Add("int", DbType.Int32);
			map.Add("long", DbType.Int64);
			map.Add("float", DbType.Single);
			return map;
		}

		static Type GetDataType(DbType type)
		{
			switch (type)
			{
				case DbType.Boolean: return typeof(bool);
				case DbType.Byte: return typeof(byte);

				case DbType.Int16: return typeof(short);
				case DbType.Int32: return typeof(int);
				case DbType.Int64: return typeof(long);
				case DbType.Single: return typeof(float);
				case DbType.Double: return typeof(double);
				case DbType.Decimal: return typeof(decimal);
				case DbType.String:
				case DbType.AnsiString: return typeof(string);
				case DbType.Binary: return typeof(byte[]);
				case DbType.Guid: return typeof(Guid);
				case DbType.DateTime:
				case DbType.Date: return typeof(DateTime);
			}
			throw new NotSupportedException();
		}

		static DbType GetColumnType(Type type)
		{
			switch (Type.GetTypeCode(type))
			{
				case TypeCode.Boolean:
					return DbType.Boolean;
				case TypeCode.Byte:
					return DbType.Byte;
				case TypeCode.Char:
					return DbType.StringFixedLength;
				case TypeCode.Int16:
				case TypeCode.SByte:
					return DbType.Int16;
				case TypeCode.Int32:
				case TypeCode.UInt16:
					return DbType.Int32;
				case TypeCode.Int64:
				case TypeCode.UInt32:
					return DbType.Int64;
				case TypeCode.Single:
					return DbType.Single;
				case TypeCode.Double:
					return DbType.Double;
				case TypeCode.Decimal:
					return DbType.Decimal;
				case TypeCode.String:
					//more?
					return DbType.String;
				case TypeCode.DateTime:
					//more?
					return DbType.DateTime;
			}

			if (type == typeof(byte[]))
			{
				return DbType.Binary;
			}

			if (type == typeof(Guid))
			{
				return DbType.Guid;
			}
			return DbType.String; //?
		}

		static bool HasLength(DbType type)
		{
			return
				type == DbType.String ||
				type == DbType.AnsiString ||
				type == DbType.Binary;
		}

		class SchemaColumn : DbColumn
		{
			const string SeriesSymbol = "*";
			public DbType DbType { get; }

			public int? SeriesOrdinal { get; }
			public string? SeriesName { get; }
			public string? SeriesHeaderFormat { get; }

			public bool? IsDateSeries { get; }
			public bool? IsIntegerSeries { get; }

			public override object? this[string property]
			{
				get
				{
					switch (property)
					{
						case nameof(DbType):
							return this.DbType;
						case nameof(SeriesOrdinal):
							return this.SeriesOrdinal;
						case nameof(SeriesName):
							return this.SeriesName;
						case nameof(SeriesHeaderFormat):
							return this.SeriesHeaderFormat;
						case nameof(IsDateSeries):
							return this.IsDateSeries;
						case nameof(IsIntegerSeries):
							return this.IsIntegerSeries;
						case nameof(Format):
							return this.Format;
						default:
							return base[property];
					}
				}
			}
						
			public string? Format { get; }

			public SchemaColumn(int? ordinal, string? baseName, string name, DbType type, bool allowNull, int? size = null, string? format = null)
			{
				this.ColumnOrdinal = ordinal;
				this.DbType = type;

#warning this feels like the wrong place to deal with series.
				bool isSeries = name?.EndsWith(SeriesSymbol) == true;

				this.SeriesOrdinal = isSeries ? 0 : (int?)null;
				this.SeriesName = isSeries ? name?.Substring(0, name.Length - 1) : null;
				this.BaseColumnName = isSeries ? null : baseName;
				this.ColumnName = isSeries ? null : name;
				this.SeriesHeaderFormat = isSeries ? baseName : null;
				this.IsDateSeries = isSeries ? name?.IndexOf("{Date}", StringComparison.OrdinalIgnoreCase) >= 0 : (bool?)null;
				this.IsIntegerSeries = isSeries ? name?.IndexOf("{Integer}", StringComparison.OrdinalIgnoreCase) >= 0 : (bool?)null;

				this.AllowDBNull = allowNull;
				this.ColumnSize = size;
				this.DataType = GetDataType(type);
				this.DataTypeName = type.ToString();
				this.Format = format;
			}

			public SchemaColumn(DbColumn col)
			{
				this.DbType = GetColumnType(col.DataType);
				this.ColumnName = col.ColumnName;
				this.ColumnOrdinal = col.ColumnOrdinal;
				this.BaseColumnName = col.BaseColumnName;
				this.DataType = col.DataType;
				this.ColumnSize = col.ColumnSize;
				this.AllowDBNull = col.AllowDBNull;
				this.IsUnique = col.IsUnique;
				this.IsLong = col.IsLong;
				this.NumericPrecision = col.NumericPrecision;
				this.NumericScale = col.NumericScale;
				this.Format = col[nameof(Format)] as string;
			}
		}

		/// <summary>
		/// Builder for creating Schema.
		/// </summary>
		public class Builder
		{
			List<SchemaColumn> columns;

			/// <summary>
			/// Creates a new Builder.
			/// </summary>
			public Builder()
			{
				this.columns = new List<SchemaColumn>();
			}

			/// <summary>
			/// Adds a column
			/// </summary>
			public Builder AddColumn(string name, DbType type, bool allowNull = true, int? size = null, string? format = null)
			{
				return this.AddColumn(null, name, type, allowNull, size, format);
			}

			/// <summary>
			/// Adds a column
			/// </summary>
			public Builder AddColumn(string? baseName, string name, DbType type, bool allowNull = true, int? size = null, string? format = null)
			{
				var col = new SchemaColumn(this.columns.Count, baseName, name, type, allowNull, size, format);
				this.columns.Add(col);
				return this;
			}

			/// <summary>
			/// Builds a Schema.
			/// </summary>
			public Schema Build()
			{
				return new Schema(columns);
			}
		}

		// types: byte,int16,int32,int64,float,double,decimal,string,binary,date,datetime,
		// Id:int;
		// FirstName:string[32]?;
		// LastName:string[32]?;
		// *:double?;
		SchemaColumn[] columns;

		private Schema(IEnumerable<SchemaColumn> cols)
		{
			this.columns = cols.ToArray();
		}

		/// <summary>
		/// Creates a Schema from the schema of an existing data reader.
		/// </summary>
		/// <param name="dataReader">The data reader to use as a schema template.</param>
		public Schema(DbDataReader dataReader) : this(dataReader.GetColumnSchema()) { }

		/// <summary>
		/// Creates a Schema from an existing schema.
		/// </summary>
		/// <param name="schema">The schema to use as a template.</param>
		public Schema(ReadOnlyCollection<DbColumn> schema)
		{
			this.columns =
				schema
				.Select(c => new SchemaColumn(c))
				.ToArray();

		}

		static readonly Regex ColSpecRegex =
			new Regex(
				@"^((?<BaseName>[^\>]+)\>)?(?<Name>[^\:]+)?(?::(?<Type>[a-z0-9]+)(\[(?<Size>\d+)\])?(?<AllowNull>\?)?(\{(?<Format>[^\}]+)\})?)?$",
				RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant
			);

		static readonly Regex NewLineRegex =
			new Regex(
				"\r\n|\n",
				RegexOptions.Multiline | RegexOptions.Compiled
			);

		/// <summary>
		/// Attempts to parse a schema specification.
		/// </summary>
		/// <param name="spec">The schema specification string.</param>
		/// <returns>A Schema, or null if it failed to parse.</returns>
		public static Schema? TryParse(string spec)
		{
			var map = ColumnTypeMap.Value;
			var colSpecs = NewLineRegex.Replace(spec, "").Split(',');
			var builder = new Builder();

			foreach (var colSpec in colSpecs)
			{
				var match = ColSpecRegex.Match(colSpec);
				if (match.Success)
				{
					var typeGroup = match.Groups["Type"];
					var formatGroup = match.Groups["Format"];
					var baseNameGroup = match.Groups["BaseName"];
					var baseName = baseNameGroup.Success ? baseNameGroup.Value : null;
					var name = WebUtility.UrlDecode(match.Groups["Name"].Value);
					DbType type = DbType.String;
					bool allowNull = true;
					int size = -1;
					if (typeGroup.Success)
					{
						var typeName = typeGroup.Value;
						allowNull = match.Groups["AllowNull"].Success;
						var sg = match.Groups["Size"];
						size = sg.Success ? int.Parse(sg.Value) : -1;
						if (!map.TryGetValue(typeName, out type))
						{
							return null;
						}
					}
					string? format = null;
					if (formatGroup.Success)
					{
						format = formatGroup.Value;
					}
					
					builder.AddColumn(baseName, name, type, allowNull, size == -1 ? null : (int?)size, format);
				}
				else
				{
					return null;
				}
			}
			return builder.Build();
		}

		/// <inheritdoc/>
		public override string ToString()
		{
			return GetSchemaSpecification(true);
		}

		/// <summary>
		/// Gets the specification string for this schema.
		/// </summary>
		/// <param name="multiline">Indicates if the spec should be singleline, or multiline.</param>
		/// <returns>A string.</returns>
		public string GetSchemaSpecification(bool multiline = false)
		{
			var w = new StringWriter();
			bool first = true;
			foreach (var col in this.columns)
			{
				if (first)
				{
					first = false;
				}
				else
				{
					w.Write(",");
					if (multiline)
					{
						w.WriteLine();
					}
				}

				if (col.BaseColumnName != null && col.BaseColumnName != col.ColumnName)
				{
					w.Write(WebUtility.UrlEncode(col.BaseColumnName));
					w.Write(">");
				}
				w.Write(WebUtility.UrlEncode(col.ColumnName));
				WriteType(w, col);
			}

			return w.ToString();
		}

		static void WriteType(TextWriter w, SchemaColumn col)
		{
			w.Write(":");
			w.Write(col.DbType.ToString());
			if (HasLength(col.DbType))
			{
				if (col.ColumnSize != null)
				{
					w.Write("[");
					w.Write(col.ColumnSize?.ToString() ?? "*");
					w.Write("]");
				}
			}
			if (col.AllowDBNull != false)
			{
				w.Write("?");
			}
		}

		public ReadOnlyCollection<DbColumn> GetColumnSchema()
		{
			return new ReadOnlyCollection<DbColumn>(this.columns);
		}
	}
}
