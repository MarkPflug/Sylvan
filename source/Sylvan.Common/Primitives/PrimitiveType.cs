namespace Sylvan.Primitives;

/// <summary>
/// Defines the types of values that can be stored in a <see cref="Primitive"/> struct.
/// </summary>
public enum PrimitiveType : byte
{
	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds no value.
	/// </summary>
	None = 0,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a boolean value.
	/// </summary>
	Boolean,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a character value.
	/// </summary>
	Char,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a byte value.
	/// </summary>
	Byte,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds an Int16 value.
	/// </summary>
	Int16,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds an Int32 value.
	/// </summary>
	Int32,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds an Int64 value.
	/// </summary>
	Int64,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds an SByte value.
	/// </summary>
	SByte,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a UInt16 value.
	/// </summary>
	UInt16,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a UInt32 value.
	/// </summary>
	UInt32,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a UInt64 value.
	/// </summary>
	UInt64,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a float value.
	/// </summary>
	Float,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a double value.
	/// </summary>
	Double,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a DateTime value.
	/// </summary>
	DateTime,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a DateTimeOffset value.
	/// </summary>
	DateTimeOffset,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a TimeSpan value.
	/// </summary>
	TimeSpan,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a Decimal value.
	/// </summary>
	Decimal,

	/// <summary>
	/// Indicates that the <see cref="TypedPrimitive"/> holds a Guid value.
	/// </summary>
	Guid,
}
