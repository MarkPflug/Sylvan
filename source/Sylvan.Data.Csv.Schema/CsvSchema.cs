using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// Provides schema information for CSV data.
	/// </summary>
	public sealed partial class CsvSchema : ICsvSchemaProvider
	{
		/// <summary>
		/// Defines the supported column types for CSV schema.
		/// </summary>
		public enum ColumnType
		{
			/// <summary>
			/// A boolean value.
			/// </summary>
			Boolean = 1,
			/// <summary>
			/// A single unsigned byte.
			/// </summary>
			Byte,
			/// <summary>
			/// A single character, equivalent to .NET Char type.
			/// </summary>
			Char,
			/// <summary>
			/// A signed 16 bit integer.
			/// </summary>
			Int16,
			/// <summary>
			/// A signed 32 bit integer.
			/// </summary>
			Int32 = 7,
			/// <summary>
			/// A signed 64 bit integer.
			/// </summary>
			Int64,
			/// <summary>
			/// A single precision floating point number.
			/// </summary>
			Float,
			/// <summary>
			/// A double precision floating point number.
			/// </summary>
			Double,
			/// <summary>
			/// A .NET Decimal value.
			/// </summary>
			Decimal,
			/// <summary>
			/// A string.
			/// </summary>
			String,
			/// <summary>
			/// A string containing only ASCII characters.
			/// </summary>
			AsciiString,
			/// <summary>
			/// A binary value.
			/// </summary>
			Binary,
			/// <summary>
			/// A unique identifier.
			/// </summary>
			Guid,
			/// <summary>
			/// A DateTime value.
			/// </summary>
			DateTime,
			/// <summary>
			/// A DateTime value representing only a date, without a time.
			/// </summary>
			Date,
		}

		static readonly Dictionary<string, ColumnType> ColumnTypeMap = 
			new Dictionary<string, ColumnType>(StringComparer.OrdinalIgnoreCase)
			{
				{"int", ColumnType.Int32 },
				{"string", ColumnType.String },
			};

		static Type GetDataType(ColumnType type)
		{
			switch (type)
			{
				case ColumnType.Boolean: return typeof(bool);
				case ColumnType.Byte: return typeof(byte);
				case ColumnType.Char: return typeof(char);
				case ColumnType.Int16: return typeof(short);
				case ColumnType.Int32: return typeof(int);
				case ColumnType.Int64: return typeof(long);
				case ColumnType.Float: return typeof(float);
				case ColumnType.Double: return typeof(double);
				case ColumnType.Decimal: return typeof(decimal);
				case ColumnType.String:
				case ColumnType.AsciiString: return typeof(string);
				case ColumnType.Binary: return typeof(byte[]);
				case ColumnType.Guid: return typeof(Guid);
				case ColumnType.DateTime:
				case ColumnType.Date: return typeof(DateTime);
			}
			throw new NotSupportedException();
		}

#warning Type, or DbColumn, we could inspect the dataTypeName to discriminate more precision
#warning Handle nullable value types?
		static ColumnType GetColumnType(Type type)
		{
			switch (Type.GetTypeCode(type))
			{
				case TypeCode.Boolean:
					return ColumnType.Boolean;
				case TypeCode.Byte:
					return ColumnType.Byte;
				case TypeCode.Char:
					return ColumnType.Char;
				case TypeCode.Int16:
				case TypeCode.SByte:
					return ColumnType.Int16;
				case TypeCode.Int32:
				case TypeCode.UInt16:
					return ColumnType.Int32;
				case TypeCode.Int64:
				case TypeCode.UInt32:
					return ColumnType.Int64;
				case TypeCode.Single:
					return ColumnType.Float;
				case TypeCode.Double:
					return ColumnType.Double;
				case TypeCode.Decimal:
					return ColumnType.Decimal;
				case TypeCode.String:
					//more?
					return ColumnType.String;
				case TypeCode.DateTime:
					//more?
					return ColumnType.DateTime;
			}

			if (type == typeof(byte[]))
			{
				return ColumnType.Binary;
			}

			if (type == typeof(Guid))
			{
				return ColumnType.Guid;
			}
			return ColumnType.String; //?
		}

		static bool HasLength(ColumnType type)
		{
			return
				type == ColumnType.String ||
				type == ColumnType.AsciiString ||
				type == ColumnType.Binary;
		}

		class CsvSchemaColumn : DbColumn
		{
			public ColumnType ColumnType { get; }

			public CsvSchemaColumn(string name, ColumnType type, bool allowNull)
			{
				this.ColumnType = type;
				this.ColumnName = name;
				this.AllowDBNull = allowNull;
				this.ColumnSize = null;
				this.DataType = GetDataType(type);
				this.DataTypeName = type.ToString();
			}

			public CsvSchemaColumn(DbColumn col)
			{
				this.ColumnType = GetColumnType(col.DataType);
				this.ColumnName = col.ColumnName;
				this.BaseColumnName = col.BaseColumnName;
				this.DataType = col.DataType;
				this.ColumnSize = col.ColumnSize;
				this.AllowDBNull = col.AllowDBNull;
				this.IsUnique = col.IsUnique;
				this.IsLong = col.IsLong;
				this.NumericPrecision = col.NumericPrecision;
				this.NumericScale = col.NumericScale;
			}
		}

		/// <summary>
		/// Builder for creating CsvSchema.
		/// </summary>
		public class Builder
		{
			List<CsvSchemaColumn> columns;

			/// <summary>
			/// Creates a new Builder.
			/// </summary>
			public Builder()
			{
				this.columns = new List<CsvSchemaColumn>();
			}

			/// <summary>
			/// Adds a column
			/// </summary>
			public Builder AddColumn(string name, ColumnType type, bool allowNull = true, int size = -1)
			{
				var col = new CsvSchemaColumn(name, type, allowNull);
				this.columns.Add(col);
				return this;
			}

			/// <summary>
			/// Builds a CsvSchema.
			/// </summary>
			public CsvSchema Build()
			{
				return new CsvSchema(columns);
			}
		}

		// types: byte,int16,int32,int64,float,double,decimal,string,binary,date,datetime,
		// Id:int;
		// FirstName:string[32]?;
		// LastName:string[32]?;
		// *:double?;

		Dictionary<string, CsvSchemaColumn> namedColumns;
		CsvSchemaColumn[] columns;
		CsvSchemaColumn? rest;

		private CsvSchema(IEnumerable<CsvSchemaColumn> cols)
		{
			this.columns = cols.ToArray();
			this.namedColumns = cols.Where(c => c.ColumnName != null).ToDictionary(c => c.ColumnName, c => c);
		}

		/// <summary>
		/// Creates a CsvSchema from the schema of an existing data reader.
		/// </summary>
		/// <param name="dataReader">The data reader to use as a schema template.</param>
		public CsvSchema(DbDataReader dataReader) : this(dataReader.GetColumnSchema()) { }

		/// <summary>
		/// Creates a CsvSchema from an existing schema.
		/// </summary>
		/// <param name="schema">The schema to use as a template.</param>
		public CsvSchema(ReadOnlyCollection<DbColumn> schema)
		{
			this.columns =
				schema
				.Select(c => new CsvSchemaColumn(c))
				.ToArray();

			this.namedColumns =
				columns
				.Where(c => c.BaseColumnName != null)
				.ToDictionary(c => c.BaseColumnName, c => c);

			this.rest = null;
		}


		static readonly Regex ColSpecRegex =
			new Regex(
				@"^(?<BaseName>[^\>]+\>)?(?<Name>[^\:]+)(?::(?<Type>[a-z]+)(\[\d+\])?(?<AllowNull>\?)?)$",
				RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant
			);

		static readonly Regex NewLineRegex =
			new Regex(
				"\r\n|\n",
				RegexOptions.Multiline | RegexOptions.Compiled
			);

		/// <summary>
		/// Attempts to parse a CSV schema specification.
		/// </summary>
		/// <param name="spec">The schema specification string.</param>
		/// <returns>A CsvSchema, or null if it failed to parse.</returns>
		public static CsvSchema? TryParse(string spec)
		{
			var colSpecs = NewLineRegex.Replace(spec, "").Split(',');
			var builder = new Builder();

			foreach (var colSpec in colSpecs)
			{
				var match = ColSpecRegex.Match(colSpec);
				if (match.Success)
				{
					var name = match.Groups["Name"].Value;
					var typeName = match.Groups["Type"].Value;
					var allowNull = match.Groups["AllowNull"].Success;
					if (ColumnTypeMap.TryGetValue(typeName, out var type)) {
						builder.AddColumn(name, type, allowNull);
						continue;
					}
				}
				return null;
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
			foreach (var col in this.columns)
			{
				if (col.BaseColumnName != null && col.BaseColumnName != col.ColumnName)
				{
#warning encoding
					w.Write(col.BaseColumnName);
					w.Write(">");
				}
				w.Write(col.ColumnName);
				WriteType(w, col);

				w.Write(",");
				if (multiline)
				{
					w.WriteLine();
				}
			}

			if (rest != null)
			{
				w.Write("*");
				WriteType(w, rest);
				w.Write(",");
				if (multiline)
				{
					w.WriteLine();
				}
			}

			return w.ToString();
		}

		static void WriteType(TextWriter w, CsvSchemaColumn col)
		{
			w.Write(":");
			w.Write(col.ColumnType.ToString());
			if (HasLength(col.ColumnType))
			{
				w.Write("[");
				w.Write(col.ColumnSize?.ToString() ?? "*");
				w.Write("]");
			}
			if (col.AllowDBNull != false)
			{
				w.Write("?");
			}
		}

		DbColumn? ICsvSchemaProvider.GetColumn(string? name, int ordinal)
		{
			if (name != null && namedColumns!.TryGetValue(name, out var col))
			{
				return col;
			}

			if (ordinal <= columns!.Length)
			{
				return columns[ordinal];
			}

			return ordinal >= columns.Length ? rest : null;
		}
	}
}
