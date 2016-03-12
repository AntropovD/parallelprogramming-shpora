using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace QuantumOfSwitching
{
	class Program
	{
		static void Main(string[] args)
		{
			Process.GetCurrentProcess().ProcessorAffinity = (IntPtr)1;
			int threadId = 0;

			var fDtCounter = 0;
			var fDts = new List<DateTime>(10 * 1000);

			var sDtCounter = 0;
			var sDts = new List<DateTime>(10 * 1000); 

			var firstThread = new Thread(() =>
			{
				var fid = Thread.CurrentThread.ManagedThreadId;
				while(true)
				{
					if(threadId != fid)
					{
						fDts.Add(DateTime.UtcNow);
						threadId = fid;
					}

				}
			});

			var secondThread = new Thread(() =>
			{
				var sid = Thread.CurrentThread.ManagedThreadId;
				while(true)
				{
					if(threadId != sid)
					{
						sDts.Add(DateTime.UtcNow);
						threadId = sid;
					}
				}
			});

			firstThread.Start();
			secondThread.Start();

			Thread.Sleep(1000);

			firstThread.Abort();
			secondThread.Abort();

			var mergedDts = fDts.Concat(sDts).OrderBy(dt => dt).ToArray();

			for(int i = 0; i < mergedDts.Length - 1; i++)
			{
				Console.WriteLine(mergedDts[i+1].Subtract(mergedDts[i]).TotalMilliseconds);
			}
				
		}
	}
}
