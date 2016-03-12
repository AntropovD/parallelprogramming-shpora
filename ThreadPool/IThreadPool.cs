using System;

namespace ThreadPool
{
	public interface IThreadPool
	{
		void EnqueueAction(Action action);
		long GetTasksProcessedCount();

		long GetWastedCycles();
	}
}