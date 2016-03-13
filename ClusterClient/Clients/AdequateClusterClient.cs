using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    class AdequateClusterClient : ClusterClientBase
    {
        public AdequateClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
            ReplicaRequestsCount = new Dictionary<string, int>();
        }

        private Dictionary<string, int> ReplicaRequestsCount;
        
        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var myUri = GetAdequateReplica();
            var webRequest = CreateRequest(myUri + "?query=" + query);

            Log.InfoFormat("Processing {0}", webRequest.RequestUri);

            var resultTask = ProcessRequestInternalAsync(webRequest);
            if (ReplicaRequestsCount.ContainsKey(myUri))
                ReplicaRequestsCount[myUri]++;
            else
                ReplicaRequestsCount.Add(myUri, 1);

            await Task.WhenAny(resultTask, Task.Delay(timeout));
            if (resultTask.Status != TaskStatus.RanToCompletion)
            {
                grayList.Dict.TryAdd(myUri, DateTime.Now.Add(GrayListWaitTime));
                throw new TimeoutException();
            }
            return resultTask.Result;
        }

        private string GetAdequateReplica()
        {
            string replica;
            grayList.UpdateGrayTable();
            do { 
                replica = FindAppropriateReplica();
            } while (grayList.Dict.ContainsKey(replica));
            return replica;
        }

        private string FindAppropriateReplica()
        {
            if (ReplicaStatistics.Count != ReplicaAddresses.Length)
            {
                var rnd = new Random();
                var notUsedReplicas = ReplicaAddresses.Where(s => !ReplicaStatistics.ContainsKey(s)).ToArray();
                var resultReplica = notUsedReplicas[rnd.Next(notUsedReplicas.Length)];
                ReplicaStatistics.TryAdd(resultReplica, Tuple.Create((long)0, 0));
                return resultReplica;
            }

            int minRequestsCount = ReplicaRequestsCount.Min(pair => pair.Value);
            var minRequests = ReplicaRequestsCount.Where(pair => pair.Value == minRequestsCount).Select(pair => pair.Key);
            return ReplicaStatistics
                .Where(pair => minRequests.Contains(pair.Key))
                .OrderBy(pair => pair.Value.Item1*1.0 / pair.Value.Item2)
                .FirstOrDefault().Key;
        }

        protected override ILog Log => LogManager.GetLogger(typeof (AdequateClusterClient));
    }
}
