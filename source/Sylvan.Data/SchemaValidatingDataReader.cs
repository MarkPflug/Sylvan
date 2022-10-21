using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace Sylvan.Data;

/// <summary>
/// Defines a method for handling schema violations.
/// </summary>
public delegate bool SchemaViolationErrorHandler(SchemaValidationContext context);

/// <summary>
/// Provides contextual information about records that violate schema requirements.
/// </summary>
public sealed class SchemaValidationContext
{
	readonly SchemaValidatingDataReader.Friend accessor;

	internal SchemaValidationContext(SchemaValidatingDataReader.Friend accessor)
	{
		this.accessor = accessor;
	}

	/// <summary>
	/// The data reader being validated.
	/// </summary>
	public DbDataReader DataReader => accessor.Reader;

	/// <summary>
	/// Gets the ordinals that contained errors.
	/// </summary>
	public IEnumerable<int> GetErrors()
	{
		return accessor.GetErrors();
	}

	/// <summary>
	/// Gets the exception that was thrown when processing the column.
	/// </summary>
	/// <param name="ordinal">The column ordinal.</param>
	/// <returns>An exception or null.</returns>
	public Exception? GetException(int ordinal)
	{
		return accessor.GetException(ordinal);
	}

	/// <summary>
	/// Sets the value for a field.
	/// </summary>
	public void SetValue<T>(int ordinal, T value)
	{
		this.accessor.SetValue(ordinal, value);
	}

	/// <summary>
	/// Sets the value for a field.
	/// </summary>
	public void SetValue(int ordinal, object value)
	{
		this.accessor.SetValue(ordinal, value);
	}
}

class SchemaValidatingDataReader : DataReaderAdapter
{
	// TODO: name this. Just meant to limit visibility to internals
	internal struct Friend
	{
		readonly SchemaValidatingDataReader reader;

		public Friend(SchemaValidatingDataReader reader)
		{
			this.reader = reader;
		}

		public DbDataReader Reader => reader.inner;

		public IEnumerable<int> GetErrors()
		{
			var markers = reader.errorMarker;
			for (int i = 0; i < markers.Length; i++)
			{
				if (markers[i] == reader.counter)
					yield return i;
			}
		}

		public Exception? GetException(int ordinal)
		{
			return 
				reader.errorMarker[ordinal] == reader.counter
				? reader.exceptions[ordinal]
				: null;
		}

		public void SetValue<T>(int ordinal, T? value)
		{
			var r = this.reader.cache[ordinal];
			if (r is ValueRef<T> vr)
			{
				vr.Value = value;
			}
			else
			{
				r.SetUserValue(value);
			}
		}

		public void SetValue(int ordinal, object? value)
		{
			var r = this.reader.cache[ordinal];
			r.SetUserValue(value);
		}
	}

	abstract class Ref
	{
		public bool IsNull { get; set; }

		public abstract object GetValue();
		public abstract void SetUserValue(object? value);

		public virtual void Reset()
		{
			this.IsNull = true;
		}
	}

	class ValueRef<T> : Ref
	{
		public T? Value { get; set; }

		public override object GetValue()
		{
			return ((object?)this.Value) ?? DBNull.Value;
		}

		public override void SetUserValue(object? value)
		{
			var isNull = value == null;
			this.IsNull = isNull;
			if (isNull)
			{
				this.Value = default;
			}
			else
			{
				this.Value = (T)value!;
			}
		}

		public override void Reset()
		{
			this.Value = default;
			base.Reset();
		}
	}

	abstract class ValueAccessor
	{
		public abstract Ref CreateRef();

		public abstract void Process(DbDataReader reader, int ordinal, Ref value);
	}

	abstract class ValueAccessor<T> : ValueAccessor
	{
		public override Ref CreateRef()
		{
			return new ValueRef<T>();
		}

		public override void Process(DbDataReader reader, int ordinal, Ref value)
		{
			var rv = (ValueRef<T>)value;
			ProcessValue(reader, ordinal, rv);
		}

		public abstract void ProcessValue(DbDataReader reader, int ordinal, ValueRef<T> value);
	}

	static class Accessors
	{
		public static ValueAccessor Boolean = new GenericValueAccessor<bool>((r, o) => r.GetBoolean(o));
		public static ValueAccessor Byte = new GenericValueAccessor<byte>((r, o) => r.GetByte(o));
		public static ValueAccessor Int16 = new GenericValueAccessor<short>((r, o) => r.GetInt16(o));
		public static ValueAccessor Int32 = new GenericValueAccessor<int>((r, o) => r.GetInt32(o));
		public static ValueAccessor Int64 = new GenericValueAccessor<long>((r, o) => r.GetInt64(o));

		public static ValueAccessor Char = new GenericValueAccessor<char>((r, o) => r.GetChar(o));
		public static ValueAccessor String = new GenericValueAccessor<string>((r, o) => r.GetString(o));

		public static ValueAccessor Single = new GenericValueAccessor<float>((r, o) => r.GetFloat(o));
		public static ValueAccessor Double = new GenericValueAccessor<double>((r, o) => r.GetDouble(o));
		public static ValueAccessor Decimal = new GenericValueAccessor<decimal>((r, o) => r.GetDecimal(o));

		public static ValueAccessor DateTime = new GenericValueAccessor<DateTime>((r, o) => r.GetDateTime(o));

		public static ValueAccessor Guid = new GenericValueAccessor<Guid>((r, o) => r.GetGuid(o));

		public static ValueAccessor Object = new GenericValueAccessor<object>((r, o) => r.GetValue(o));
	}

	sealed class BytesValueAccessor : ValueAccessor<byte[]>
	{
		readonly byte[] buffer;

		public BytesValueAccessor()
		{
			this.buffer = new byte[1];
		}

		public override void ProcessValue(DbDataReader reader, int ordinal, ValueRef<byte[]> value)
		{
			value.Value = null;
			reader.GetBytes(ordinal, 0, buffer, 0, 1);
			value.Value = buffer;
		}
	}

	sealed class CharsValueAccessor : ValueAccessor<char[]>
	{
		readonly char[] buffer;

		public CharsValueAccessor()
		{
			this.buffer = new char[1];
		}

		public override void ProcessValue(DbDataReader reader, int ordinal, ValueRef<char[]> value)
		{
			value.Value = null;
			reader.GetChars(ordinal, 0, buffer, 0, 1);
			value.Value = buffer;
		}
	}

	sealed class GenericValueAccessor<T> : ValueAccessor<T>
	{
		readonly Func<DbDataReader, int, T> accessor;

		public GenericValueAccessor(Func<DbDataReader, int, T> accessor)
		{
			this.accessor = accessor;
		}

		public override void ProcessValue(DbDataReader reader, int ordinal, ValueRef<T> value)
		{
			value.Value = accessor(reader, ordinal);
		}
	}

	// the cache of values for the current row populated during calls to Read()
	Ref[] cache;
	ValueAccessor[] accessors;
	int[] errorMarker;
	Exception[] exceptions;
	int counter;

	readonly DbDataReader inner;
	readonly SchemaValidationContext validationContext;
	readonly SchemaViolationErrorHandler errorHandler;

	static bool Fail(SchemaValidationContext context)
	{
		return false;
	}

	internal SchemaValidatingDataReader(DbDataReader inner, SchemaViolationErrorHandler? errorHandler = null)
		: base(inner)
	{
		this.inner = inner;
		this.errorHandler = errorHandler ?? Fail;
		this.cache = Array.Empty<Ref>();
		this.accessors = Array.Empty<ValueAccessor>();
		this.errorMarker = Array.Empty<int>();
		this.exceptions = Array.Empty<Exception>();
		this.validationContext = new SchemaValidationContext(new Friend(this));
		this.counter = -1;
		InitializeSchema();
	}

	void InitializeSchema()
	{
		var count = inner.FieldCount;
		if (cache.Length < count)
		{
			this.cache = new Ref[count];
			this.accessors = new ValueAccessor[count];
			this.errorMarker = new int[count];
			this.exceptions = new Exception[count];
		}

		var schema = inner.GetColumnSchema();
		for (int i = 0; i < count; i++)
		{
			var t = schema[i].DataType;
			var acc = GetAccessor(t);
			this.accessors[i] = acc;
			this.cache[i] = acc.CreateRef();
			this.errorMarker[i] = -1;
		}
	}

	static ValueAccessor GetAccessor(Type? type)
	{
		if (type == null) return Accessors.Object;

		var code = Type.GetTypeCode(type);
		switch (code)
		{
			case TypeCode.Boolean:
				return Accessors.Boolean;
			case TypeCode.Byte:
				return Accessors.Byte;
			case TypeCode.Int16:
				return Accessors.Int16;
			case TypeCode.Int32:
				return Accessors.Int32;
			case TypeCode.Int64:
				return Accessors.Int64;

			case TypeCode.Single:
				return Accessors.Single;
			case TypeCode.Double:
				return Accessors.Double;
			case TypeCode.Decimal:
				return Accessors.Decimal;

			case TypeCode.Char:
				return Accessors.Char;
			case TypeCode.String:
				return Accessors.String;
			case TypeCode.Object:
			default:
				if (type == typeof(DateTime))
				{
					return Accessors.DateTime;
				}
				if (type == typeof(Guid))
				{
					return Accessors.Guid;
				}
				if (type == typeof(byte[]))
				{
					return new BytesValueAccessor();
				}
				if (type == typeof(char[]))
				{
					return new CharsValueAccessor();
				}
				throw new NotSupportedException();
		}
	}

	bool ProcessRow()
	{
		counter++;
		var errorCount = 0;
		for (int i = 0; i < inner.FieldCount; i++)
		{
			var r = cache[i];
			r.Reset();
			try
			{
				this.accessors[i].Process(this.inner, i, r);
			}
			catch (Exception ex)
			{
				errorCount++;
				this.exceptions[i] = ex;
				this.errorMarker[i] = counter;
			}
		}
		if (errorCount > 0)
		{
			if (errorHandler(this.validationContext))
			{
				// TODO: re-validate ?
			}
			else
			{
				return false;
			}
		}

		return true;
	}

	public override object this[int ordinal] => GetValue(ordinal);

	public override object this[string name] => this.GetValue(GetOrdinal(name));

	public override int Depth => 0;

	public override int FieldCount => inner.FieldCount;

	public override bool HasRows => inner.HasRows; // TODO: we can't answer this correctly, right?

	public override bool IsClosed => inner.IsClosed;

	public override int RecordsAffected => 0;

	T GetRefValue<T>(int ordinal)
	{
		ValidateOrdinal(ordinal);
		return ((ValueRef<T>)cache[ordinal]).Value!;
	}

	public override bool GetBoolean(int ordinal)
	{
		return GetRefValue<bool>(ordinal);
	}

	public override byte GetByte(int ordinal)
	{
		return GetRefValue<byte>(ordinal);
	}

	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
	{
		return inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
	}

	public override char GetChar(int ordinal)
	{
		return GetRefValue<char>(ordinal);
	}

	public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
	{
		return inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
	}

	public override string GetDataTypeName(int ordinal)
	{
		return inner.GetDataTypeName(ordinal);
	}

	public override DateTime GetDateTime(int ordinal)
	{
		return GetRefValue<DateTime>(ordinal);
	}

	public override decimal GetDecimal(int ordinal)
	{
		return GetRefValue<decimal>(ordinal);
	}

	public override double GetDouble(int ordinal)
	{
		return GetRefValue<double>(ordinal);
	}

	public override IEnumerator GetEnumerator()
	{
		throw new NotSupportedException();
	}

	public override Type GetFieldType(int ordinal)
	{
		return inner.GetFieldType(ordinal);
	}

	public override float GetFloat(int ordinal)
	{
		return GetRefValue<float>(ordinal);
	}

	public override Guid GetGuid(int ordinal)
	{
		return GetRefValue<Guid>(ordinal);
	}

	public override short GetInt16(int ordinal)
	{
		return GetRefValue<short>(ordinal);
	}

	public override int GetInt32(int ordinal)
	{
		return GetRefValue<int>(ordinal);
	}

	public override long GetInt64(int ordinal)
	{
		return GetRefValue<long>(ordinal);
	}

	public override string GetName(int ordinal)
	{
		return inner.GetName(ordinal);
	}

	public override int GetOrdinal(string name)
	{
		return inner.GetOrdinal(name);
	}

	public override string GetString(int ordinal)
	{
		return GetRefValue<string>(ordinal);
	}

	public override object GetValue(int ordinal)
	{
		if (ordinal < 0 || ordinal >= this.FieldCount)
			throw new ArgumentOutOfRangeException(nameof(ordinal));
		return cache[ordinal].GetValue();
	}

	public override int GetValues(object?[] values)
	{
		var count = Math.Min(this.FieldCount, values.Length);
		for (int i = 0; i < count; i++)
		{
			values[i] = GetValue(i);
		}
		return count;
	}

	void ValidateOrdinal(int ordinal)
	{
		if (ordinal < 0 || ordinal >= this.FieldCount)
			throw new ArgumentOutOfRangeException(nameof(ordinal));
	}

	public override bool IsDBNull(int ordinal)
	{
		ValidateOrdinal(ordinal);
		return cache[ordinal].IsNull;
	}

	public override bool NextResult()
	{
		if (inner.NextResult())
		{
			InitializeSchema();
			return true;
		}
		return false;
	}

	public override bool Read()
	{
		while (inner.Read())
		{
			// if the current row has valid data
			if (ProcessRow())
			{
				// return it
				return true;
			}
			// otherwise move to the next row
		}
		return false;
	}
}
