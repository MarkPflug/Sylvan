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

	Boolean,

	Char,

	Byte,
	Int16,
	Int32,
	Int64,

	SByte,
	UInt16,
	UInt32,
	UInt64,

	Float,
	Double,

	DateTime,
	DateTimeOffset,
	TimeSpan,

	Decimal,
	Guid,
}
