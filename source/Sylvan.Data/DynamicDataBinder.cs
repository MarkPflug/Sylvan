using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace Sylvan.Data
{

	static class ILEmitExtensions
	{
		static ConstructorInfo NieCtor = typeof(NotImplementedException).GetConstructor(Array.Empty<Type>())!;

		public static ILGenerator ThrowNotImplemented(this ILGenerator gen)
		{
			gen.Emit(OpCodes.Newobj, NieCtor);
			gen.Emit(OpCodes.Throw);
			return gen;
		}
	}

	public static class ObjectBinder
	{
		public static IDataBinderFactory<T> Get<T>()
		{
			return BinderCache<T>.Instance;
		}
	}

	public static class NullAccessor<TP>
	{
		public static Func<DbDataReader, int, TP> Instance;

		static NullAccessor()
		{
			Instance = (dr, i) => default!;
		}
	}

	public static class BinderAccessor
	{
		public static Func<DbDataReader, int, T> GetAccessor<T>()
		{
			return (r, i) => ((DbDataReader)r).GetFieldValue<T>(i);
		}
	}

	static class BinderCache<T>
	{
		internal static IDataBinderFactory<T> Instance;

		static BinderCache()
		{
			Instance = BuildFactory();
		}

		static readonly Type SchemaType = typeof(IReadOnlyList<DbColumn>);

		static IDataBinderFactory<T> BuildFactory()
		{
			var type = typeof(T);
			var accType = typeof(Func<,,>);

			var binderType = typeof(IDataBinder<>).MakeGenericType(new Type[] { type });
			var binderFactoryType = typeof(IDataBinderFactory<>).MakeGenericType(new Type[] { type });

			var attrs = TypeAttributes.Class | TypeAttributes.Sealed;
			var mb = BinderBuilder.mb;

			var builder = mb.DefineType("Sylvan.Data.Generated.Binder_" + type.Name, attrs, typeof(object), new Type[] { typeof(IDataBinder), binderType });

			var ctor = builder.DefineConstructor(MethodAttributes.Public, CallingConventions.HasThis, new Type[] { SchemaType });
			var ctorIL = ctor.GetILGenerator();
			var iVar = ctorIL.DeclareLocal(typeof(int));
			var enuVar = ctorIL.DeclareLocal(typeof(IEnumerator<DbColumn>));
			var colVar = ctorIL.DeclareLocal(typeof(DbColumn));
			var ordinalVar = ctorIL.DeclareLocal(typeof(int));


			var mapType = typeof(Dictionary<string, int>);
			var mapField = builder.DefineField("Map", mapType, FieldAttributes.Private | FieldAttributes.Static);

			Debug.Assert(iVar.LocalIndex == 0);
			ctorIL.Emit(OpCodes.Ldc_I4_0);
			ctorIL.Emit(OpCodes.Stloc_0);

			Dictionary<string, int> map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

			var props = GetBindableProperties(type).ToArray();
			Label loopLabel = ctorIL.DefineLabel();
			Label startLabel = ctorIL.DefineLabel();
			Label[] propLabels = new Label[props.Length];
			for (int i = 0; i < propLabels.Length; i++)
			{
				propLabels[i] = ctorIL.DefineLabel();
			}
			Label defaultLabel = ctorIL.DefineLabel();
			int idx = 0;

			var idxFields = new FieldInfo[props.Length];
			var accFields = new FieldInfo[props.Length];

			// initialize fields with default values.
			foreach (var prop in props)
			{
				map.Add(prop.Name, idx);

				var idxField = builder.DefineField("idx" + idx, typeof(int), FieldAttributes.Private);
				var accessorType = accType.MakeGenericType(typeof(DbDataReader), typeof(int), prop.PropertyType);
				var accField = builder.DefineField("accessor" + idx, accessorType, FieldAttributes.Private);
				idxFields[idx] = idxField;
				accFields[idx] = accField;

				// set idxField to -1
				ctorIL.Emit(OpCodes.Ldarg_0);
				ctorIL.Emit(OpCodes.Ldc_I4_M1);
				ctorIL.Emit(OpCodes.Stfld, idxField);

				ctorIL.Emit(OpCodes.Ldarg_0);
				var nullAccType = typeof(NullAccessor<>).MakeGenericType(prop.PropertyType);
				var accInstField = nullAccType.GetField("Instance", BindingFlags.Public | BindingFlags.Static)!;
				ctorIL.Emit(OpCodes.Ldsfld, accInstField);
				ctorIL.Emit(OpCodes.Stfld, accField);

				idx++;
			}

			ctorIL.Emit(OpCodes.Ldc_I4_0);
			ctorIL.Emit(OpCodes.Stloc_0);

			ctorIL.Emit(OpCodes.Ldarg_1); // load schema

			ctorIL.Emit(OpCodes.Callvirt, typeof(IEnumerable<DbColumn>).GetMethod("GetEnumerator")!);
			ctorIL.Emit(OpCodes.Stloc_1);

			ctorIL.Emit(OpCodes.Br, loopLabel);

			ctorIL.MarkLabel(startLabel);
			ctorIL.Emit(OpCodes.Ldloc_1);
			ctorIL.Emit(OpCodes.Callvirt, typeof(IEnumerator<DbColumn>).GetProperty("Current")!.GetGetMethod()!);
			ctorIL.Emit(OpCodes.Stloc_2);

			ctorIL.Emit(OpCodes.Ldsfld, mapField); // static
			ctorIL.Emit(OpCodes.Ldloc_2);
			ctorIL.Emit(OpCodes.Callvirt, typeof(DbColumn).GetProperty("ColumnName")!.GetGetMethod()!);
			ctorIL.Emit(OpCodes.Ldloca_S, ordinalVar.LocalIndex);
			ctorIL.Emit(OpCodes.Callvirt, mapType.GetMethod("TryGetValue")!);
			ctorIL.Emit(OpCodes.Brfalse, defaultLabel);

			ctorIL.Emit(OpCodes.Ldloc_3);
			ctorIL.Emit(OpCodes.Switch, propLabels);
			ctorIL.Emit(OpCodes.Br, defaultLabel);

			for (int i = 0; i < props.Length; i++)
			{
				var prop = props[i];
				ctorIL.MarkLabel(propLabels[i]);
				// this.idx{i} = i;
				ctorIL.Emit(OpCodes.Ldarg_0);
				ctorIL.Emit(OpCodes.Ldloc_0);
				ctorIL.Emit(OpCodes.Stfld, idxFields[i]);

				// this.acc{i} = 
				ctorIL.Emit(OpCodes.Ldarg_0);
				//ctorIL.Emit(OpCodes.Ldloc_2);
				var accMethod = typeof(BinderAccessor).GetMethod("GetAccessor", BindingFlags.Public | BindingFlags.Static)!;
				accMethod = accMethod.MakeGenericMethod(prop.PropertyType);
				ctorIL.Emit(OpCodes.Call, accMethod);

				ctorIL.Emit(OpCodes.Stfld, accFields[i]);
				ctorIL.Emit(OpCodes.Br, defaultLabel);
			}

			ctorIL.MarkLabel(defaultLabel);
			ctorIL.Emit(OpCodes.Ldloc_0);
			ctorIL.Emit(OpCodes.Ldc_I4_1);
			ctorIL.Emit(OpCodes.Add);
			ctorIL.Emit(OpCodes.Stloc_0);

			ctorIL.MarkLabel(loopLabel);
			ctorIL.Emit(OpCodes.Ldloc_1);
			ctorIL.Emit(OpCodes.Callvirt, typeof(System.Collections.IEnumerator).GetMethod("MoveNext")!);
			ctorIL.Emit(OpCodes.Brtrue, startLabel);


			ctorIL.Emit(OpCodes.Ret);


			var methodAttrs = MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig;
			var method = builder.DefineMethod("Bind", methodAttrs, typeof(void), new Type[] { typeof(DbDataReader), type });
			var mIL = method.GetILGenerator();

			idx = 0;
			foreach(var prop in props)
			{
				mIL.Emit(OpCodes.Ldarg_2);
				mIL.Emit(OpCodes.Ldarg_0);
				var accField = accFields[idx];
				mIL.Emit(OpCodes.Ldfld, accField);

				mIL.Emit(OpCodes.Ldarg_1);
				mIL.Emit(OpCodes.Ldarg_0);
				
				mIL.Emit(OpCodes.Ldfld, idxFields[idx]);
				var accMethod = accField.FieldType.GetMethod("Invoke", new Type[] { typeof(DbDataReader), typeof(int) })!;
				mIL.Emit(OpCodes.Callvirt, accMethod);
				mIL.Emit(OpCodes.Callvirt, prop.GetSetMethod()!);
				idx++;
			}
			mIL.Emit(OpCodes.Ret);

			var bindMethod = binderType.GetMethod("Bind");
			builder.DefineMethodOverride(method, bindMethod!);

			method = builder.DefineMethod("Bind", methodAttrs, typeof(void), new Type[] { typeof(DbDataReader), typeof(object) });
			mIL = method.GetILGenerator();
			mIL.Emit(OpCodes.Ldarg_0);
			mIL.Emit(OpCodes.Ldarg_1);
			mIL.Emit(OpCodes.Castclass, type);
			mIL.Emit(OpCodes.Callvirt, bindMethod!);
			mIL.Emit(OpCodes.Ret);
			bindMethod = typeof(IDataBinder).GetMethod("Bind");
			builder.DefineMethodOverride(method, bindMethod!);

			var bT = builder.CreateTypeInfo()!;

			var facBuilder = mb.DefineType("Sylvan.Data.Generated.BinderFactory_" + type.Name, attrs, typeof(object), new Type[] { binderFactoryType });
			var createMethod = facBuilder.DefineMethod("Create", methodAttrs, binderType, new Type[] { SchemaType, typeof(DataBinderOptions) });

			mIL = createMethod.GetILGenerator();
			mIL.Emit(OpCodes.Ldarg_1);
			var binderCtor = bT.GetConstructor(new Type[] { SchemaType })!;
			mIL.Emit(OpCodes.Newobj, binderCtor);
			mIL.Emit(OpCodes.Ret);

			var facType = facBuilder.CreateTypeInfo()!;

			var rtMapField = bT.GetField(mapField.Name, BindingFlags.Static | BindingFlags.NonPublic)!;
			rtMapField.SetValue(null, map);

			return (IDataBinderFactory<T>)Activator.CreateInstance(facType)!;
		}

		static IEnumerable<PropertyInfo> GetBindableProperties(Type type)
		{
			var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
			foreach(var prop in props)
			{
				if (prop.GetSetMethod() == null) continue;
				yield return prop;
			}
		}
	}

	static class BinderBuilder
	{
		const string Name = "SylvanDataBinder";

		internal static AssemblyBuilder ab;
		internal static ModuleBuilder mb;

		static BinderBuilder()
		{
			ab = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName(Name), AssemblyBuilderAccess.Run);
			mb = ab.DefineDynamicModule(Name);
		}
	}
}
