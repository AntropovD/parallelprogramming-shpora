using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    class SmartClusterClient : ClusterClientBase
    {
        public SmartClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var replicaSequence = GetStatisticsReplicaSequence();

            double replicaTimeout = timeout.TotalMilliseconds / ReplicaAddresses.Length;

            var timeoutTask = Task.Delay(timeout);
            var tasks = new List<Task>();
            tasks.Add(timeoutTask);
            foreach (var replica in replicaSequence)
            {
                foreach (var task in tasks)
                {
                    if (task == timeoutTask && task.Status==TaskStatus.RanToCompletion)
                        throw new TimeoutException();
                    if (task.Status == TaskStatus.RanToCompletion)
                        return ((Task<string>) task).Result;
                }

                var webRequest = CreateRequest($"{replica}?query={query}");
                Log.InfoFormat("Processing {0}", webRequest.RequestUri);

                var currentTask = ProcessRequestInternalAsync(webRequest);
                await Task.WhenAny(currentTask, Task.Delay(TimeSpan.FromMilliseconds(replicaTimeout)));
                if (currentTask.Status != TaskStatus.RanToCompletion)
                {
                    if (!currentTask.IsCompleted)
                    {
                        grayList.Dict.TryAdd(grayList.FormatKey(webRequest.RequestUri), DateTime.Now.Add(GrayListWaitTime));
                        continue;
                    }
                    tasks.Add(currentTask);
                }
                return currentTask.Result;
            }
            foreach (var task in tasks)
            {
                if (task == timeoutTask)
                    throw new TimeoutException();
                if (task.Status == TaskStatus.RanToCompletion)
                    return ((Task<string>)task).Result;
            }
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}
