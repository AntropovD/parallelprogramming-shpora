using System;
using System.Threading;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    class RoundRobinClusterClient : ClusterClientBase
    {
        public RoundRobinClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public async override Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var replicaSequence = GetStatisticsReplicaSequence();
                 
            double replicaTimeout = timeout.TotalMilliseconds / ReplicaAddresses.Length;

            foreach (var replica in replicaSequence)
            {
                var webRequest = CreateRequest($"{replica}?query={query}");
                Log.InfoFormat("Processing {0}", webRequest.RequestUri);

                var resultTask = ProcessRequestInternalAsync(webRequest);

                var timeoutTask = Task.Delay(TimeSpan.FromMilliseconds(replicaTimeout));
                var finishedTask = await Task.WhenAny(resultTask, Task.Delay(TimeSpan.FromMilliseconds(replicaTimeout)));
                if (finishedTask == timeoutTask)
                {
                    grayList.Dict.TryAdd(grayList.FormatKey(webRequest.RequestUri), DateTime.Now.Add(GrayListWaitTime));
                    continue;
                }
                if (resultTask.Status != TaskStatus.RanToCompletion)
                    continue;
                return resultTask.Result;
            }
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}
