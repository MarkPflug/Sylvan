using Sylvan.Terminal;

namespace TestConsole
{
	class Program
    {
        static void Main(string[] args)
        {

			var c = new ColorConsole();
			c.SetForeground(0xa0, 0xa0, 0x20);
			c.Write("Hello, world");
        }
    }
}
