using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace ThreadPool
{
	public class SimpleLockThreadPool : IThreadPool
	{
		public long wastedTicks = 0;

		public SimpleLockThreadPool() : this(Environment.ProcessorCount) { }

		public SimpleLockThreadPool(int concurrencyLevel)
		{
			if(concurrencyLevel <= 0)
				throw new ArgumentOutOfRangeException("concurrencyLevel");

			this.concurrencyLevel = concurrencyLevel;
		}

		private readonly int concurrencyLevel;

		private readonly Queue<Action> globalQueue = new Queue<Action>();

		[ThreadStatic] private static int thisThreadIndex;
		private long[] counters;

		private Thread[] threads;
		private int threadsWaiting;

		public void EnqueueAction(Action action)
		{
			EnsureStarted();

			lock (globalQueue)
			{
				globalQueue.Enqueue(action);
				if(threadsWaiting > 0)
					Monitor.Pulse(globalQueue);
			}
		}

		public long GetTasksProcessedCount()
		{
			return counters != null ? counters.Sum() : 0;
		}

		public long GetWastedCycles()
		{
			return wastedTicks;
		}

		private void EnsureStarted()
		{
			if(threads == null)
			{
				lock (globalQueue)
				{
					if(threads == null)
					{
						threads = new Thread[concurrencyLevel];
						counters = new long[concurrencyLevel];
						for(int i = 0; i < threads.Length; i++)
						{
							var threadIndex = i;
							threads[i] = new Thread(() => DispatchLoop(threadIndex)) {IsBackground = true};
							threads[i].Start();
						}
					}
				}
			}
		}

		private void DispatchLoop(int threadIndex)
		{
			thisThreadIndex = threadIndex;

			var sw = new Stopwatch();
			while(true)
			{
				Action action;
				sw.Restart();
				lock (globalQueue)
				{
					sw.Stop();
					Interlocked.Add(ref wastedTicks, sw.ElapsedTicks);
					while(globalQueue.Count == 0)
					{
						threadsWaiting++;
						try
						{
							sw.Restart();
							Monitor.Wait(globalQueue);
							sw.Stop();
							Interlocked.Add(ref wastedTicks, sw.ElapsedTicks);
						}
						finally { threadsWaiting--; }

					}
					action = globalQueue.Dequeue();
				}

				action.Invoke();
				counters[thisThreadIndex]++;
			}
		}
	}
}
