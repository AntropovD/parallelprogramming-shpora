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

            var tasks = new List<Task<string>>();
            foreach (var replica in replicaSequence)
            {
                foreach (var task in tasks)
                    if (task.Status == TaskStatus.RanToCompletion)
                        return task.Result;

                var webRequest = CreateRequest($"{replica}?query={query}");
                Log.InfoFormat("Processing {0}", webRequest.RequestUri);

                var currentTask = ProcessRequestInternalAsync(webRequest);

                await Task.WhenAny(currentTask, Task.Delay(TimeSpan.FromMilliseconds(replicaTimeout)));
                if (currentTask.Status != TaskStatus.RanToCompletion)
                {
                    if (currentTask.IsCompleted)
                    {
                        continue;
                        
//                        GrayList.TryAdd(webRequest.RequestUri.AbsoluteUri, DateTime.Now.Add(GrayListWaitTime));
                    }
                    tasks.Add(currentTask);
                }
                return currentTask.Result;
            }
            foreach (var task in tasks)
                if (task.Status == TaskStatus.RanToCompletion)
                    return task.Result;
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}
