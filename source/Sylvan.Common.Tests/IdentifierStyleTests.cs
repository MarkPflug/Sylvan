using System.Collections.Generic;
using Xunit;

namespace Sylvan
{
	public class IdentifierStyleTests
	{
		[Theory]
		[InlineData("test", "test")]
		[InlineData("testInput", "test,Input")]
		[InlineData("test_Input", "test,Input")]
		[InlineData("_test__Input", "test,Input")]
		[InlineData("test-Input", "test,Input")]
		[InlineData("DBNull", "DB,Null")]
		[InlineData("DB_Null", "DB,Null")]
		[InlineData("DB Null", "DB,Null")]
		[InlineData("DB_NULL", "DB,NULL")]
		[InlineData("CLRType", "CLR,Type")]
		[InlineData("CLR_TYPE", "CLR,TYPE")]
		public void GetSegments(string input, string segments)
		{
			var parts = segments.Split(',');
			var segs = new List<string>();
			foreach(var segment in IdentifierStyle.GetSegments(input))
			{
				var str = input.Substring(segment.Start, segment.Length);
				segs.Add(str);
			}
			Assert.Equal(parts, segs);
		}

		[Theory]
		[InlineData("test", "Test")]
		[InlineData("testInput", "TestInput")]
		[InlineData("test_Input", "TestInput")]
		[InlineData("_test__Input", "TestInput")]
		[InlineData("test-Input", "TestInput")]
		[InlineData("IsDBNull", "IsDBNull")]
		[InlineData("DBNull", "DBNull")]
		[InlineData("DB_Null", "DBNull")]
		[InlineData("DB_NULL", "DbNull")]
		[InlineData("DB Null", "DBNull")]
		[InlineData("CLRType", "CLRType")]
		[InlineData("CLR_TYPE", "ClrType")]
		public void PascalCase(string input, string expected)
		{
			var style = new PascalCaseStyle();
			var output = style.Convert(input);
			Assert.Equal(expected, output);
		}

		[Theory]
		[InlineData("test", "test")]
		[InlineData("testInput", "testInput")]
		[InlineData("test_Input", "testInput")]
		[InlineData("_test__Input", "testInput")]
		[InlineData("test-Input", "testInput")]
		[InlineData("IsDBNull", "isDBNull")]
		[InlineData("DBNull", "dbNull")]
		[InlineData("DB_Null", "dbNull")]
		[InlineData("DB_NULL", "dbNull")]
		[InlineData("DB Null", "dbNull")]
		[InlineData("CLRType", "clrType")]
		[InlineData("CLR_TYPE", "clrType")]
		public void CamelCase(string input, string expected)
		{
			var style = new CamelCaseStyle();
			var output = style.Convert(input);
			Assert.Equal(expected, output);
		}

		[Theory]
		[InlineData("test", "Test")]
		[InlineData("testInput", "Test_Input")]
		[InlineData("test_Input", "Test_Input")]
		[InlineData("_test__Input", "Test_Input")]
		[InlineData("test-Input", "Test_Input")]
		[InlineData("IsDBNull", "Is_DB_Null")]
		[InlineData("DBNull", "DB_Null")]
		[InlineData("DB_Null", "DB_Null")]
		[InlineData("DB_NULL", "Db_Null")]
		[InlineData("DB Null", "DB_Null")]
		[InlineData("CLRType", "CLR_Type")]
		[InlineData("CLR_TYPE", "Clr_Type")]
		public void UnderscoreStyle(string input, string expected)
		{
			var style = new UnderscoreStyle(CasingStyle.TitleCase);
			var output = style.Convert(input);
			Assert.Equal(expected, output);
		}

		[Theory]
		[InlineData("test", "test")]
		[InlineData("testInput", "test_input")]
		[InlineData("test_Input", "test_input")]
		[InlineData("_test__Input", "test_input")]
		[InlineData("test-Input", "test_input")]
		[InlineData("IsDBNull", "is_db_null")]
		[InlineData("DBNull", "db_null")]
		[InlineData("DB_Null", "db_null")]
		[InlineData("DB_NULL", "db_null")]
		[InlineData("DB Null", "db_null")]
		[InlineData("CLRType", "clr_type")]
		[InlineData("CLR_TYPE", "clr_type")]
		public void UnderscoreLowerStyle(string input, string expected)
		{
			var style = new UnderscoreStyle(CasingStyle.LowerCase);
			var output = style.Convert(input);
			Assert.Equal(expected, output);
		}

		[Theory]
		[InlineData("test", "\"test\"")]
		[InlineData("testInput", "\"test input\"")]
		[InlineData("test_Input", "\"test input\"")]
		[InlineData("_test__Input", "\"test input\"")]
		[InlineData("test-Input", "\"test input\"")]
		[InlineData("IsDBNull", "\"is db null\"")]
		[InlineData("DBNull", "\"db null\"")]
		[InlineData("DB_Null", "\"db null\"")]
		[InlineData("DB_NULL", "\"db null\"")]
		[InlineData("DB Null", "\"db null\"")]
		[InlineData("CLRType", "\"clr type\"")]
		[InlineData("CLR_TYPE", "\"clr type\"")]
		public void QuotedIdentifierStyle(string input, string expected)
		{
			var style = new QuotedIdentifierStyle(CasingStyle.LowerCase, ' ');
			var output = style.Convert(input);
			Assert.Equal(expected, output);
		}
	}
}
