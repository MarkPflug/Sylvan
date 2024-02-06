﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data;

/// <summary>
/// Provides methods for constructing DbDataReader instances
/// over object collections.
/// </summary>
public static class ObjectDataReader
{
	/// <summary>
	/// Creates a DbDataReader over a sequence of objects.
	/// </summary>
	public static DbDataReader Create<T>(IEnumerable<T> data)
	{
		return new SyncObjectDataReader<T>(data);
	}

#if IAsyncEnumerable

	/// <summary>
	/// Creates a DbDataReader over a sequence of objects.
	/// </summary>
	public static DbDataReader Create<T>(IAsyncEnumerable<T> data, CancellationToken cancel = default)
	{
		return new AsyncObjectDataReader<T>(data, cancel);
	}

#endif

	/// <summary>
	/// Creates an ObjectDataReader.Builder for the provided data.
	/// </summary>
	/// <remarks>
	/// This overload is useful when the type T is an anonymous type.
	/// </remarks>
	public static Builder<T> CreateBuilder<T>(IEnumerable<T> data)
	{
		return new Builder<T>();
	}

	/// <summary>
	/// Creates an ObjectDataReader.Builder for reading data from the type T.
	/// </summary>
	public static Builder<T> CreateBuilder<T>()
	{
		return new Builder<T>();
	}

	/// <summary>
	/// A builder for constructing DbDataReader over object data.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public sealed class Builder<T>
	{
		readonly ObjectDataReader<T>.Builder builder;

		/// <summary>
		/// Creates a new ObjectDataReader.Builder{T}.
		/// </summary>
		public Builder()
		{
			this.builder = new ObjectDataReader<T>.Builder();
		}

		/// <summary>
		/// Adds a column to the DbDataReader.
		/// </summary>
		public Builder<T> AddColumn<T0>(string name, Func<T, T0?> func) where T0 : struct
		{
			this.builder.AddColumn(name, func);
			return this;
		}

		/// <summary>
		/// Adds a column to the DbDataReader.
		/// </summary>
		public Builder<T> AddColumn<T0>(string name, Func<T, T0> func)
		{
			this.builder.AddColumn(name, func);
			return this;
		}

		/// <summary>
		/// Adds all public properties to the DbDataReader.
		/// </summary>
		public Builder<T> AddAllProperties()
		{
			this.builder.AddDefaultColumns();
			return this;
		}

		/// <summary>
		/// Builds a DbDataReader over the data that will read the columns defined by the builder.
		/// </summary>
		public DbDataReader Build(IEnumerable<T> data)
		{
			return builder.Build(data);
		}
	}
}

sealed class SyncObjectDataReader<T> : ObjectDataReader<T>
{
	readonly IEnumerator<T> enumerator;

	public SyncObjectDataReader(IEnumerable<T> seq)
	{
		this.enumerator = seq.GetEnumerator();
	}

	public SyncObjectDataReader(IEnumerable<T> seq, ColumnInfo[] cols)
		: base(cols)
	{
		this.enumerator = seq.GetEnumerator();
	}

	public override bool Read()
	{
		return this.enumerator.MoveNext();
	}

	public override Task<bool> ReadAsync(CancellationToken cancel)
	{
		cancel.ThrowIfCancellationRequested();
		return Task.FromResult(Read());
	}

	public override T Current => this.enumerator.Current;
}

#if IAsyncEnumerable

sealed class AsyncObjectDataReader<T> : ObjectDataReader<T>
{
	readonly IAsyncEnumerator<T> enumerator;

	public AsyncObjectDataReader(IAsyncEnumerable<T> seq, CancellationToken cancel)
	{
		this.enumerator = seq.GetAsyncEnumerator(cancel);
	}

	public AsyncObjectDataReader(IAsyncEnumerable<T> seq, ColumnInfo[] cols, CancellationToken cancel)
		: base(cols)
	{
		this.enumerator = seq.GetAsyncEnumerator(cancel);
	}

	public override bool Read()
	{
		throw new NotSupportedException();
	}

	public override Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();
		return this.enumerator.MoveNextAsync().AsTask();
	}

	public override T Current => this.enumerator.Current;
}

#endif

abstract class ObjectDataReader<T> : DbDataReader, IDbColumnSchemaGenerator
{
	internal static Lazy<Builder> DefaultBuilder =
		new(() => new Builder().AddDefaultColumns());

	internal sealed class Builder
	{
		internal List<ColumnInfo> columns;

		internal Builder()
		{
			this.columns = new List<ColumnInfo>();
		}

		static Builder()
		{
			var addMethods =
				typeof(Builder)
				.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
				.Where(m => m.Name == "AddColumn")
				.ToArray();

			AddNullableMethod =
				addMethods.Single(
					m =>
					{
						var p = m.GetParameters()[1];
						var t = p.ParameterType.GetGenericArguments()[1];
						var def = t.IsGenericType ? t.GetGenericTypeDefinition() : null;
						return def == typeof(Nullable<>);
					}
				);
			AddMethod =
				addMethods.Single(m => m != AddNullableMethod);
		}

		static readonly MethodInfo AddNullableMethod;
		static readonly MethodInfo AddMethod;

		// bool, short, int, long, single, double, decimal, dateTime, guid, string

		internal Builder AddColumn<T0>(string name, Func<T, T0?> func) where T0 : struct
		{
			Func<T, T0> selector = item => func(item)!.Value;
			Func<T, object> valueSelector = item => { var r = func(item); return r.HasValue ? (object)r.Value : DBNull.Value; };
			Func<T, bool> isNullSelector = item => !func(item).HasValue;
			this.columns.Add(new ColumnInfo(columns.Count, name, typeof(T0?), selector, valueSelector, isNullSelector));
			return this;
		}

		internal Builder AddColumn<T0>(string name, Func<T, T0> func)
		{
			Func<T, object> valueSelector = item => { var r = func(item); return (object?)r ?? DBNull.Value; };
			Func<T, bool> isNullSelector =
				typeof(T0).IsValueType
				? (Func<T, bool>)(item => false)
				: (Func<T, bool>)(item => func(item) == null);
			this.columns.Add(new ColumnInfo(columns.Count, name, typeof(T0), func, valueSelector, isNullSelector));
			return this;
		}

		internal Builder AddDefaultColumns()
		{
			var t = typeof(T);
			var props = t.GetProperties();
			foreach (var prop in props)
			{
				var getter = prop.GetGetMethod();
				if (getter == null || !IsSupported(prop)) continue;
				AddProperty(prop);
			}

			return this;
		}

		static bool IsSupported(PropertyInfo prop)
		{
			return prop.GetIndexParameters().Length == 0 && IsSupported(prop.PropertyType);
		}

		static readonly HashSet<Type> SupportedTypes = new()
		{
			typeof(string),
			typeof(byte[]),
			typeof(char[]),
			typeof(Guid),
			typeof(DateTime),
			typeof(DateTimeOffset),
			typeof(TimeSpan),
			typeof(decimal),
			typeof(Type),

#if NET6_0_OR_GREATER
			typeof(DateOnly),
			typeof(TimeOnly),
#endif
		};

		static bool IsSupported(Type type)
		{
			if (SupportedTypes.Contains(type)) return true;

			if (type.IsArray) return false;
			if (type.IsPrimitive) return true;
			if (type.IsEnum) return true;

			var nt = Nullable.GetUnderlyingType(type);
			if (nt != null)
			{
				if (Nullable.GetUnderlyingType(nt) != null) return false;
				return IsSupported(nt);
			}

			return false;
		}

		internal Builder AddProperty(PropertyInfo prop)
		{
			var propType = prop.PropertyType;
			var getter = prop.GetGetMethod();
			if (getter == null) throw new ArgumentException(nameof(prop));
			var param = Expression.Parameter(typeof(T));
			var parameters = new ParameterExpression[] { param };
			var expr = Expression.Lambda(Expression.Call(param, getter), parameters);
			var selector = expr.Compile();

			var baseType = Nullable.GetUnderlyingType(propType);

			var attr = prop.GetCustomAttribute(typeof(DataMemberAttribute)) as DataMemberAttribute;

			var name = attr?.Name ?? prop.Name;

			if (baseType == null)
			{
				AddMethod.MakeGenericMethod(new Type[] { propType }).Invoke(this, new object[] { name, selector });
			}
			else
			{
				AddNullableMethod.MakeGenericMethod(new Type[] { baseType }).Invoke(this, new object[] { name, selector });
			}
			return this;
		}

		internal DbDataReader Build(IEnumerable<T> data)
		{
			return new SyncObjectDataReader<T>(data, this.columns.ToArray());
		}
	}

	internal class ColumnInfo : DbColumn
	{
		internal object selector;
		internal Func<T, object> valueSelector;
		internal Func<T, bool> isNullSelector;
		internal TypeCode typeCode;

		public ColumnInfo(int ordinal, string name, Type type, object selector, Func<T, object> valueSelector, Func<T, bool> isNullSelector)
		{
			this.selector = selector;
			this.valueSelector = valueSelector;
			this.isNullSelector = isNullSelector;
			this.typeCode = Type.GetTypeCode(type);
			this.ColumnOrdinal = ordinal;
			this.ColumnName = name;
			var nullableBase = Nullable.GetUnderlyingType(type);
			if (nullableBase != null)
			{
				this.DataType = nullableBase;
				this.AllowDBNull = true;
			}
			else
			{
				this.DataType = type;
				this.AllowDBNull = this.DataType.IsValueType == false;
			}
			this.DataTypeName = this.DataType.Name;
		}
	}


	readonly ColumnInfo[] columns;
	bool isClosed;

	protected ObjectDataReader() : this(DefaultBuilder.Value.columns.ToArray())
	{
	}

	protected ObjectDataReader(ColumnInfo[] columns)
	{
		this.columns = columns;
		this.isClosed = false;
	}

	public override bool IsClosed
	{
		get { return this.isClosed; }
	}

	public override void Close()
	{
		this.isClosed = true;
	}

	public override bool NextResult()
	{
		return false;
	}

	public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
	{
		return Task.FromResult(false);
	}

	public abstract override bool Read();

	public abstract override Task<bool> ReadAsync(CancellationToken cancellationToken);

	public abstract T Current { get; }

	public override string GetName(int ordinal)
	{
		return this.columns[ordinal].ColumnName;
	}

	public override Type GetFieldType(int ordinal)
	{
		return this.columns[ordinal].DataType!;
	}

	public override int GetOrdinal(string name)
	{
		for (int i = 0; i < this.columns.Length; i++)
			if (string.Compare(columns[i].ColumnName, name, false) == 0)
				return i;

		for (int i = 0; i < this.columns.Length; i++)
			if (string.Compare(columns[i].ColumnName, name, true) == 0)
				return i;

		throw new ArgumentOutOfRangeException(nameof(name));
	}

	public override bool GetBoolean(int ordinal)
	{
		return GetFieldValue<bool>(ordinal);
	}

	public override TValue GetFieldValue<TValue>(int ordinal)
	{
		var col = columns[ordinal];
		if (col.selector is Func<T, TValue> b)
		{
			return b(Current);
		}
		else
		{
			var obj = col.valueSelector(Current);
			if (obj != DBNull.Value && obj is TValue val)
			{
				return val;
			}
		}
		throw new InvalidCastException();
	}

	public override byte GetByte(int ordinal)
	{
		return GetFieldValue<byte>(ordinal);
	}

	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
	{
		if (buffer == null) throw new ArgumentNullException(nameof(buffer));
		if (dataOffset > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(dataOffset));
		var offset = (int)dataOffset;
		// TODO: consider caching the result of GetFieldValue between calls to GetBytes.
		// If the selector allocates, this could cause very bad performance.
		// Or, maybe 
		var data = GetFieldValue<byte[]>(ordinal);

		var len = Math.Min(data.Length - offset, length);
		Array.Copy(data, offset, buffer, bufferOffset, len);
		return len;
	}

	public override char GetChar(int ordinal)
	{
		return GetFieldValue<char>(ordinal);
	}

	public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
	{
		if (buffer == null) throw new ArgumentNullException(nameof(buffer));
		if (dataOffset > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(dataOffset));
		var off = (int)dataOffset;
		var str = this.GetString(ordinal);
		var len = Math.Min(str.Length - off, length);
		str.CopyTo((int)dataOffset, buffer, bufferOffset, len);
		return len;
	}

	public override string GetDataTypeName(int ordinal)
	{
		return GetFieldType(ordinal).Name;
	}

	public override DateTime GetDateTime(int ordinal)
	{
		return GetFieldValue<DateTime>(ordinal);
	}

	public override decimal GetDecimal(int ordinal)
	{
		return GetFieldValue<decimal>(ordinal);
	}

	public override double GetDouble(int ordinal)
	{
		return GetFieldValue<double>(ordinal);
	}

	public override IEnumerator GetEnumerator()
	{
		while (this.Read())
			yield return this;
	}

	public override float GetFloat(int ordinal)
	{
		return GetFieldValue<float>(ordinal);
	}

	public override Guid GetGuid(int ordinal)
	{
		return GetFieldValue<Guid>(ordinal);
	}

	public override short GetInt16(int ordinal)
	{
		return GetFieldValue<short>(ordinal);
	}

	public override int GetInt32(int ordinal)
	{
		return GetFieldValue<int>(ordinal);
	}

	public override long GetInt64(int ordinal)
	{
		return GetFieldValue<long>(ordinal);
	}

	public override string GetString(int ordinal)
	{
		var col = columns[ordinal];
		var cur = Current;
		if (col.selector is Func<T, string> s)
		{
			return s(cur);
		}
		return col.valueSelector(cur)?.ToString() ?? "";
	}

	public override object GetValue(int ordinal)
	{
		var value = this.columns[ordinal].valueSelector(Current);
		return value;
	}

	public override int GetValues(object[] values)
	{
		var c = Math.Min(this.FieldCount, values.Length);
		for (int i = 0; i < c; i++)
		{
			values[i] = GetValue(i);
		}
		return c;
	}

	public override bool IsDBNull(int ordinal)
	{
		var col = this.columns[ordinal];
		if (col.AllowDBNull == false) return false;
		return col.isNullSelector(Current);
	}

	public ReadOnlyCollection<DbColumn> GetColumnSchema()
	{
		return new ReadOnlyCollection<DbColumn>(this.columns);
	}

	public override System.Data.DataTable GetSchemaTable()
	{
		return SchemaTable.GetSchemaTable(GetColumnSchema());
	}

	public override int FieldCount
	{
		get { return this.columns.Length; }
	}

	public override int Depth => 0;

	public override bool HasRows => true;

	public override int RecordsAffected => -1;

	public override object this[string name] => GetValue(this.GetOrdinal(name));

	public override object this[int ordinal] => this.GetValue(ordinal);
}
