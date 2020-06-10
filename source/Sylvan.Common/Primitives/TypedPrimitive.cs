using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.InteropServices;

namespace Sylvan.Primitives
{
	/// <summary>
	/// Expands upon the <see cref="Primitive"/> structure to provide information about the value
	/// contained.
	/// </summary>
	/// <remarks>
	/// The size of this structure is 20 bytes. This is 4 bytes larger than the <see
	/// cref="Primitive"/> type. It can be more efficient to
	/// use <see cref="Primitive"/> directly, when the <see cref="PrimitiveType"/> value can be
	/// stored externally.
	/// </remarks>
	[SuppressMessage("Design", "CA1065:Do not raise exceptions in unexpected locations", Justification = "<Pending>")]
	[SuppressMessage("Usage", "CA2225:Operator overloads have named alternates", Justification = "<Pending>")]
	[StructLayout(LayoutKind.Explicit, Size = 20)]
	public struct TypedPrimitive : IEquatable<TypedPrimitive>
	{
		[FieldOffset(0)]
		Primitive value;
		[FieldOffset(16)]
		PrimitiveType type;

		public PrimitiveType Type
		{
			get { return this.type; }
		}

		public Primitive Value
		{
			get { return this.value; }
		}

		#region value accessors

		public bool BoolValue
		{
			get
			{
				if (this.type == PrimitiveType.Boolean)
					return this.value.BoolValue;
				throw new InvalidCastException();
			}
		}
		public char CharValue
		{
			get
			{
				if (this.type == PrimitiveType.Char)
					return this.value.CharValue;
				throw new InvalidCastException();
			}
		}

		public byte ByteValue
		{
			get
			{
				if (this.type == PrimitiveType.Byte)
					return this.value.ByteValue;
				throw new InvalidCastException();
			}
		}

		public sbyte SByteValue
		{
			get
			{
				if (this.type == PrimitiveType.SByte)
					return this.value.SByteValue;
				throw new InvalidCastException();
			}
		}

		public short Int16Value
		{
			get
			{
				if (this.type == PrimitiveType.Int16)
					return this.value.Int16Value;
				throw new InvalidCastException();
			}
		}

		public ushort UInt16Value
		{
			get
			{
				if (this.type == PrimitiveType.UInt16)
					return this.value.UInt16Value;
				throw new InvalidCastException();
			}
		}

		public int Int32Value
		{
			get
			{
				if (this.type == PrimitiveType.Int32)
					return this.value.Int32Value;
				throw new InvalidCastException();
			}
		}

		public uint UInt32Value
		{
			get
			{
				if (this.type == PrimitiveType.UInt32)
					return this.value.UInt32Value;
				throw new InvalidCastException();
			}
		}

		public long Int64Value
		{
			get
			{
				if (this.type == PrimitiveType.Int64)
					return this.value.Int64Value;
				throw new InvalidCastException();
			}
		}

		public ulong UInt64Value
		{
			get
			{
				if (this.type == PrimitiveType.UInt64)
					return this.value.UInt64Value;
				throw new InvalidCastException();
			}
		}

		public float FloatValue
		{
			get
			{
				if (this.type == PrimitiveType.Float)
					return this.value.FloatValue;
				throw new InvalidCastException();
			}
		}

		public double DoubleValue
		{
			get
			{
				if (this.type == PrimitiveType.Double)
					return this.value.DoubleValue;
				throw new InvalidCastException();
			}
		}

		public DateTime DateTimeValue
		{
			get
			{
				if (this.type == PrimitiveType.DateTime)
					return this.value.DateTimeValue;
				throw new InvalidCastException();
			}
		}

		public TimeSpan TimeSpanValue
		{
			get
			{
				if (this.type == PrimitiveType.TimeSpan)
					return this.value.TimeSpanValue;
				throw new InvalidCastException();
			}
		}


		public decimal DecimalValue
		{
			get
			{
				if (this.type == PrimitiveType.Decimal)
					return this.value.DecimalValue;
				throw new InvalidCastException();
			}
		}

		public Guid GuidValue
		{
			get
			{
				if (this.type == PrimitiveType.Guid)
					return this.value.GuidValue;
				throw new InvalidCastException();
			}
		}

		#endregion

		#region equality and hashing

		public override int GetHashCode()
		{
			return this.type.GetHashCode() ^ this.value.GetHashCode();
		}

		public override bool Equals(object obj)
		{
			return obj is TypedPrimitive && Equals((TypedPrimitive)obj);
		}

		public bool Equals(TypedPrimitive obj)
		{
			return this.type == obj.type && this.value.Equals(obj.value);
		}

		public override string ToString()
		{
			return this.ToString(CultureInfo.CurrentCulture);
		}

		public string ToString(CultureInfo culture)
		{
			return value.ToString(type, culture) + ":" + type.ToString();
		}

		public static bool operator ==(TypedPrimitive p1, TypedPrimitive p2)
		{
			return p1.type == p2.type && p1.value.Equals(p2.value);
		}

		public static bool operator !=(TypedPrimitive p1, TypedPrimitive p2)
		{
			return !(p1 == p2);
		}

		#endregion

		#region constructors

		public static TypedPrimitive Unbox(object value)
		{
			if (value == null) throw new ArgumentNullException(nameof(value));
			var t = value.GetType();
			var code = System.Type.GetTypeCode(t);

			switch (code)
			{
				case TypeCode.Boolean:
					return new TypedPrimitive((bool)value);
				case TypeCode.Byte:
					return new TypedPrimitive((byte)value);
				case TypeCode.SByte:
					return new TypedPrimitive((sbyte)value);
				case TypeCode.Int16:
					return new TypedPrimitive((short)value);
				case TypeCode.UInt16:
					return new TypedPrimitive((ushort)value);
				case TypeCode.Int32:
					return new TypedPrimitive((int)value);
				case TypeCode.UInt32:
					return new TypedPrimitive((uint)value);
				case TypeCode.Int64:
					return new TypedPrimitive((long)value);
				case TypeCode.UInt64:
					return new TypedPrimitive((ulong)value);
				case TypeCode.Single:
					return new TypedPrimitive((float)value);
				case TypeCode.Double:
					return new TypedPrimitive((double)value);
				case TypeCode.Decimal:
					return new TypedPrimitive((decimal)value);
				case TypeCode.DateTime:
					return new TypedPrimitive((DateTime)value);
				case TypeCode.Object:
					if (t == typeof(Guid))
						return new TypedPrimitive((Guid)value);
					if (t == typeof(TimeSpan))
						return new TypedPrimitive((TimeSpan)value);
					break;
			}
			throw new InvalidCastException();
		}

		internal TypedPrimitive(PrimitiveType type, Primitive value)
		{
			this.type = type;
			this.value = value;
		}

		public TypedPrimitive(bool value) : this(PrimitiveType.Boolean, value)
		{
		}

		public TypedPrimitive(char value) : this(PrimitiveType.Char, value)
		{
		}

		public TypedPrimitive(byte value) : this(PrimitiveType.Byte, value)
		{
		}

		public TypedPrimitive(sbyte value) : this(PrimitiveType.SByte, value)
		{
		}

		public TypedPrimitive(short value) : this(PrimitiveType.Int16, value)
		{
		}

		public TypedPrimitive(ushort value) : this(PrimitiveType.UInt16, value)
		{
		}

		public TypedPrimitive(int value) : this(PrimitiveType.Int32, value)
		{
		}

		public TypedPrimitive(uint value) : this(PrimitiveType.UInt32, value)
		{
		}

		public TypedPrimitive(long value) : this(PrimitiveType.Int64, value)
		{
		}

		public TypedPrimitive(ulong value) : this(PrimitiveType.UInt64, value)
		{
		}

		public TypedPrimitive(float value) : this(PrimitiveType.Float, value)
		{
		}

		public TypedPrimitive(double value) : this(PrimitiveType.Double, value)
		{
		}

		public TypedPrimitive(DateTime value) : this(PrimitiveType.DateTime, value)
		{
		}

		public TypedPrimitive(DateTimeOffset value) : this(PrimitiveType.DateTimeOffset, value)
		{
		}

		public TypedPrimitive(TimeSpan value) : this(PrimitiveType.TimeSpan, value)
		{
		}

		public TypedPrimitive(decimal value) : this(PrimitiveType.Decimal, value)
		{
		}

		public TypedPrimitive(Guid value) : this(PrimitiveType.Guid, value)
		{
		}

		#endregion

		
		public static implicit operator TypedPrimitive(bool value)
		{
			return new TypedPrimitive(value);
		}

		public static explicit operator bool(TypedPrimitive primitive)
		{
			return primitive.BoolValue;
		}

		public static implicit operator TypedPrimitive(byte value)
		{
			return new TypedPrimitive(value);
		}

		public static explicit operator byte(TypedPrimitive primitive)
		{
			return primitive.ByteValue;
		}

		public static implicit operator TypedPrimitive(short value)
		{
			return new TypedPrimitive(value);
		}

		public static explicit operator short(TypedPrimitive primitive)
		{
			return primitive.Int16Value;
		}

		public static implicit operator TypedPrimitive(int value)
		{
			return new TypedPrimitive(value);
		}

		public static explicit operator int(TypedPrimitive primitive)
		{
			return primitive.Int32Value;
		}

		public static implicit operator TypedPrimitive(long value)
		{
			return new TypedPrimitive(value);
		}

		public static explicit operator long(TypedPrimitive primitive)
		{
			return primitive.Int64Value;
		}

		public static implicit operator TypedPrimitive(DateTime value)
		{
			return new TypedPrimitive(value);
		}

		public static explicit operator DateTime(TypedPrimitive primitive)
		{
			return primitive.DateTimeValue;
		}

		public static implicit operator TypedPrimitive(TimeSpan value)
		{
			return new TypedPrimitive(value);
		}

		public static explicit operator TimeSpan(TypedPrimitive primitive)
		{
			return primitive.TimeSpanValue;
		}

		public static implicit operator TypedPrimitive(decimal value)
		{
			return new TypedPrimitive(value);
		}

		public static explicit operator decimal(TypedPrimitive primitive)
		{
			return primitive.DecimalValue;
		}

		public static implicit operator TypedPrimitive(Guid value)
		{
			return new TypedPrimitive(value);
		}

		public static explicit operator Guid(TypedPrimitive primitive)
		{
			return primitive.GuidValue;
		}
	}
}
