using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq.Expressions;

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

	// A reusable strongly-typed box to store row values.
	// This is used to avoid boxing every single value.
	class ValueRef<T> : Ref
	{
		public T? Value
		{
			get; set;
		}

		public override object GetValue()
		{
			return ((object?)this.Value) ?? DBNull.Value;
		}

		public override void SetUserValue(object? value)
		{
			var isNull = value == null || value == DBNull.Value;
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

		public static ValueAccessor NullableBoolean = new GenericNullableValueAccessor<bool>((r, o) => r.GetBoolean(o));
		public static ValueAccessor NullableByte = new GenericNullableValueAccessor<byte>((r, o) => r.GetByte(o));
		public static ValueAccessor NullableInt16 = new GenericNullableValueAccessor<short>((r, o) => r.GetInt16(o));
		public static ValueAccessor NullableInt32 = new GenericNullableValueAccessor<int>((r, o) => r.GetInt32(o));
		public static ValueAccessor NullableInt64 = new GenericNullableValueAccessor<long>((r, o) => r.GetInt64(o));
		public static ValueAccessor NullableChar = new GenericNullableValueAccessor<char>((r, o) => r.GetChar(o));
		public static ValueAccessor NullableString = new GenericNullableValueAccessor<string>((r, o) => r.GetString(o));
		public static ValueAccessor NullableSingle = new GenericNullableValueAccessor<float>((r, o) => r.GetFloat(o));
		public static ValueAccessor NullableDouble = new GenericNullableValueAccessor<double>((r, o) => r.GetDouble(o));
		public static ValueAccessor NullableDecimal = new GenericNullableValueAccessor<decimal>((r, o) => r.GetDecimal(o));
		public static ValueAccessor NullableDateTime = new GenericNullableValueAccessor<DateTime>((r, o) => r.GetDateTime(o));
		public static ValueAccessor NullableGuid = new GenericNullableValueAccessor<Guid>((r, o) => r.GetGuid(o));

		public static ValueAccessor Object = new GenericNullableValueAccessor<object>((r, o) => r.GetValue(o));

		public static ValueAccessor GetAccessor(Type type, bool allowNull)
		{
			var mi = typeof(DbDataReader).GetMethod("GetFieldValue");
			
			var gm = mi!.MakeGenericMethod(type);

			var drp = Expression.Parameter(typeof(DbDataReader));
			var op = Expression.Parameter(typeof(int));
			var call = Expression.Call(drp, gm, op);
			var func = Expression.Lambda(call, drp, op).Compile();
			var t = allowNull 
				? typeof(GenericNullableValueAccessor<>) 
				: typeof(GenericValueAccessor<>);

			var gt = t.MakeGenericType(type);

			var ctor = gt.GetConstructors()[0];

			var inst = ctor.Invoke(new object[] { func });

			return (ValueAccessor) inst;
		}
	}

	sealed class BytesValueAccessor : ValueAccessor<byte[]>
	{
		readonly bool nullable;
		readonly byte[] buffer;

		public BytesValueAccessor(bool nullable)
		{
			this.buffer = new byte[1];
			this.nullable = nullable;
		}

		public override void ProcessValue(DbDataReader reader, int ordinal, ValueRef<byte[]> value)
		{
			var isNull = nullable && reader.IsDBNull(ordinal);

			if (isNull)
			{
				value.Value = null;
			}
			else
			{
				reader.GetBytes(ordinal, 0, buffer, 0, 1);
				value.Value = buffer;
			}
			value.IsNull = isNull;
		}
	}

	sealed class CharsValueAccessor : ValueAccessor<char[]>
	{
		readonly bool nullable;
		readonly char[] buffer;

		public CharsValueAccessor(bool nullable)
		{
			this.buffer = new char[1];
			this.nullable = nullable;
		}

		public override void ProcessValue(DbDataReader reader, int ordinal, ValueRef<char[]> value)
		{
			var isNull = nullable && reader.IsDBNull(ordinal);
			if (isNull)
			{
				value.Value = null;
			}
			else
			{
				reader.GetChars(ordinal, 0, buffer, 0, 1);
				value.Value = buffer;
			}
			value.IsNull = isNull;
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

	sealed class GenericNullableValueAccessor<T> : ValueAccessor<T>
	{
		readonly Func<DbDataReader, int, T> accessor;

		public GenericNullableValueAccessor(Func<DbDataReader, int, T> accessor)
		{
			this.accessor = accessor;
		}

		public override void ProcessValue(DbDataReader reader, int ordinal, ValueRef<T> value)
		{
			var isNull = reader.IsDBNull(ordinal);

			value.Value = isNull ? default : accessor(reader, ordinal);
			value.IsNull = isNull;
		}
	}

	// the cache of values for the current row populated during calls to Read()
	Ref[] cache;
	ValueAccessor[] accessors;
	// stores the index of last row that encountered an error in that column.
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
			var allowNull = schema[i].AllowDBNull ?? true;
			var acc = GetAccessor(t, allowNull);
			this.accessors[i] = acc;
			this.cache[i] = acc.CreateRef();
			this.errorMarker[i] = -1;
		}
	}

	static ValueAccessor GetAccessor(Type? type, bool allowNull)
	{
		if (type == null) return Accessors.Object;

		if (type.IsEnum)
		{
			return Accessors.GetAccessor(type, allowNull);
		}

		var code = Type.GetTypeCode(type);
		switch (code)
		{
			case TypeCode.Boolean:
				return allowNull ? Accessors.NullableBoolean : Accessors.Boolean;
			case TypeCode.Byte:
				return allowNull ? Accessors.NullableByte : Accessors.Byte;
			case TypeCode.Int16:
				return allowNull ? Accessors.NullableInt16 : Accessors.Int16;
			case TypeCode.Int32:
				return allowNull ? Accessors.NullableInt32 : Accessors.Int32;
			case TypeCode.Int64:
				return allowNull ? Accessors.NullableInt64 : Accessors.Int64;

			case TypeCode.Single:
				return allowNull ? Accessors.NullableSingle : Accessors.Single;
			case TypeCode.Double:
				return allowNull ? Accessors.NullableDouble : Accessors.Double;
			case TypeCode.Decimal:
				return allowNull ? Accessors.NullableDecimal : Accessors.Decimal;

			case TypeCode.Char:
				return allowNull ? Accessors.NullableChar : Accessors.Char;
			case TypeCode.String:
				return allowNull ? Accessors.NullableString : Accessors.String;
			case TypeCode.Object:
			default:
				if (type == typeof(DateTime))
				{
					return allowNull ? Accessors.NullableDateTime : Accessors.DateTime;
				}
				if (type == typeof(Guid))
				{
					return allowNull ? Accessors.NullableGuid : Accessors.Guid;
				}
				if (type == typeof(byte[]))
				{
					return new BytesValueAccessor(allowNull);
				}
				if (type == typeof(char[]))
				{
					return new CharsValueAccessor(allowNull);
				}

				if(type == typeof(object))
				{
					return Accessors.Object;
				}

				return Accessors.GetAccessor(type, allowNull);
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

	public override ReadOnlyCollection<DbColumn> GetColumnSchema()
	{
		return Reader.GetColumnSchema();
	}
}
