using System;
using System.Runtime.InteropServices;

namespace Sylvan.Primitives
{
	/// <summary>
	/// A structure capable of holding common C# primitive types. Additionally, it can contain <see
	/// cref="DateTime"/>, <see cref="DateTimeOffset"/><see cref="TimeSpan"/> and <see cref="Guid"/>.
	/// </summary>
	/// <remarks>
	/// The size of this struct is 16 bytes regardless of what it is holding. This is the size
	/// required by the largest value type it can contain. The struct does not store the knowledge of
	/// the type of object it contains, which must be stored separately. <see cref="TypedPrimitive"/>
	/// expands on Primitive to store type information (requiring additional storage size) and
	/// provide type checks when retrieving values. Primitive should be used when a homogeneous set
	/// of values needs to be stored and it would be more efficient to store the type information
	/// externally.
	/// </remarks>
	[StructLayout(LayoutKind.Explicit)]
	public struct Primitive : IEquatable<Primitive>
	{
		[FieldOffset(0)]
		ulong lo;

		[FieldOffset(8)]
		ulong hi;

		[FieldOffset(0)]
		bool boolValue;

		[FieldOffset(0)]
		char charValue;

		[FieldOffset(0)]
		sbyte sbyteValue;

		[FieldOffset(0)]
		byte byteValue;

		[FieldOffset(0)]
		short int16Value;

		[FieldOffset(0)]
		ushort uint16Value;

		[FieldOffset(0)]
		int int32Value;

		[FieldOffset(0)]
		uint uint32Value;

		[FieldOffset(0)]
		long int64Value;

		[FieldOffset(0)]
		ulong uint64Value;

		[FieldOffset(0)]
		float floatValue;

		[FieldOffset(0)]
		double doubleValue;

		[FieldOffset(0)]
		DateTime dateTimeValue;

		[FieldOffset(0)]
		DateTimeOffset dateTimeOffsetValue;

		[FieldOffset(0)]
		TimeSpan timeSpanValue;

		[FieldOffset(0)]
		decimal decimalValue;

		[FieldOffset(0)]
		Guid guidValue;

		#region value accessors

#if NETSTANDARD21
		/// <summary>
		/// Allows access to the individual bytes in the value.
		/// </summary>
		/// <remarks>The order of the bytes will can be different depending on the endianness of the system.</remarks>
		public byte this[int idx]
		{
			get
			{
				if (idx < 0 || idx >= 16)
					throw new ArgumentOutOfRangeException(nameof(idx));
				var span = MemoryMarshal.CreateReadOnlySpan(ref this, 1);
				var data = MemoryMarshal.AsBytes(span);
				return data[idx];		
			}
		}
#endif

		public bool BoolValue => this.boolValue;

		public char CharValue => this.charValue;

		public byte ByteValue => this.byteValue;

		public sbyte SByteValue => this.sbyteValue;

		public short Int16Value => this.int16Value;

		public ushort UInt16Value => this.uint16Value;

		public int Int32Value => this.int32Value;

		public uint UInt32Value => this.uint32Value;

		public long Int64Value => this.int64Value;

		public ulong UInt64Value => this.uint64Value;

		public float FloatValue => this.floatValue;

		public double DoubleValue => this.doubleValue;

		public DateTime DateTimeValue => this.dateTimeValue;

		public DateTimeOffset DateTimeOffsetValue => this.dateTimeOffsetValue;

		public TimeSpan TimeSpanValue => this.timeSpanValue;

		public decimal DecimalValue => this.decimalValue;

		public Guid GuidValue => this.guidValue;

#endregion

#region constructors

		public Primitive(bool value)
		{
			this = default;
			this.boolValue = value;
		}

		public Primitive(char value)
		{
			this = default;
			this.charValue = value;
		}

		public Primitive(byte value)
		{
			this = default;
			this.byteValue = value;
		}

		public Primitive(sbyte value)
		{
			this = default;
			this.sbyteValue = value;
		}

		public Primitive(short value)
		{
			this = default;
			this.int16Value = value;
		}

		public Primitive(ushort value)
		{
			this = default;
			this.uint16Value = value;
		}

		public Primitive(int value)
		{
			this = default;
			this.int32Value = value;
		}

		public Primitive(uint value)
		{
			this = default;
			this.uint32Value = value;
		}

		public Primitive(long value)
		{
			this = default;
			this.int64Value = value;
		}

		public Primitive(ulong value)
		{
			this = default;
			this.uint64Value = value;
		}

		public Primitive(float value)
		{
			this = default;
			this.floatValue = value;
		}

		public Primitive(double value)
		{
			this = default;
			this.doubleValue = value;
		}

		public Primitive(DateTime value)
		{
			this = default;
			this.dateTimeValue = value;
		}

		public Primitive(DateTimeOffset value)
		{
			this = default;
			this.dateTimeOffsetValue = value;
		}

		public Primitive(TimeSpan value)
		{
			this = default;
			this.timeSpanValue = value;
		}

		public Primitive(decimal value)
		{
			this = default;
			this.decimalValue = value;
		}

		public Primitive(Guid g)
		{
			this = default;
			this.guidValue = g;
		}

#endregion

#region implicit casts

		public static implicit operator Primitive(bool value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(char value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(byte value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(sbyte value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(short value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(ushort value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(int value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(uint value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(long value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(ulong value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(float value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(double value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(DateTime value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(DateTimeOffset value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(TimeSpan value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(decimal value)
		{
			return new Primitive(value);
		}

		public static implicit operator Primitive(Guid value)
		{
			return new Primitive(value);
		}

#endregion

#region equality, hashing, tostring

		public override bool Equals(object obj)
		{
			return obj is Primitive && Equals((Primitive)obj);
		}

		public override int GetHashCode()
		{
			return lo.GetHashCode() ^ this.hi.GetHashCode();
		}

		public bool Equals(Primitive p)
		{
			return this.lo == p.lo && this.hi == p.hi;
		}

		public override string ToString()
		{
			return lo.ToString("x16") + hi.ToString("x16");
		}

		public string ToString(PrimitiveType type)
		{
			switch (type)
			{
				case PrimitiveType.None:
					return "None";
				case PrimitiveType.Boolean:
					return this.boolValue.ToString();
				case PrimitiveType.Char:
					return this.charValue.ToString();
				case PrimitiveType.Byte:
					return this.byteValue.ToString();
				case PrimitiveType.Int16:
					return this.int16Value.ToString();
				case PrimitiveType.Int32:
					return this.int32Value.ToString();
				case PrimitiveType.Int64:
					return this.int64Value.ToString();
				case PrimitiveType.SByte:
					return this.sbyteValue.ToString();
				case PrimitiveType.UInt16:
					return this.uint16Value.ToString();
				case PrimitiveType.UInt32:
					return this.uint32Value.ToString();
				case PrimitiveType.UInt64:
					return this.uint64Value.ToString();
				case PrimitiveType.Float:
					return this.floatValue.ToString();
				case PrimitiveType.Double:
					return this.doubleValue.ToString();
				case PrimitiveType.DateTime:
					return this.dateTimeValue.ToString();
				case PrimitiveType.DateTimeOffset:
					return this.dateTimeOffsetValue.ToString();
				case PrimitiveType.TimeSpan:
					return this.timeSpanValue.ToString();
				case PrimitiveType.Decimal:
					return this.decimalValue.ToString();
				case PrimitiveType.Guid:
					return this.guidValue.ToString();
				default:
					throw new NotSupportedException();
			}
		}

		public static bool operator ==(Primitive p1, Primitive p2)
		{
			return p1.Equals(p2);
		}

		public static bool operator !=(Primitive p1, Primitive p2)
		{
			return !(p1 == p2);
		}

#endregion

		public static Primitive Unbox(object obj)
		{
			var t = obj.GetType();
			var h = t.TypeHandle;

			var code = Type.GetTypeCode(t);

			switch (code)
			{
				case TypeCode.Boolean:
					return new Primitive((bool)obj);
				case TypeCode.Byte:
					return new Primitive((byte)obj);
				case TypeCode.SByte:
					return new Primitive((sbyte)obj);
				case TypeCode.Int16:
					return new Primitive((short)obj);
				case TypeCode.UInt16:
					return new Primitive((ushort)obj);
				case TypeCode.Int32:
					return new Primitive((int)obj);
				case TypeCode.UInt32:
					return new Primitive((uint)obj);
				case TypeCode.Int64:
					return new Primitive((long)obj);
				case TypeCode.UInt64:
					return new Primitive((ulong)obj);
				case TypeCode.Single:
					return new Primitive((float)obj);
				case TypeCode.Double:
					return new Primitive((double)obj);
				case TypeCode.Decimal:
					return new Primitive((decimal)obj);
				case TypeCode.DateTime:
					return new Primitive((DateTime)obj);
				case TypeCode.Object:
					if (t == typeof(Guid))
						return new Primitive((Guid)obj);
					if (t == typeof(TimeSpan))
						return new Primitive((TimeSpan)obj);
					if (t == typeof(DateTimeOffset))
						return new Primitive((DateTimeOffset)obj);
					break;
			}
			throw new InvalidCastException();
		}
	}
}
