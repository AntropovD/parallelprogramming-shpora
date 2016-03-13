using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    class AllClusterClient : ClusterClientBase
    {
        public AllClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public async override Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var tasks = new List<Task>();
            foreach (var replicaAddress in ReplicaAddresses)
            {
                var webRequest = CreateRequest($"{replicaAddress}?query={query}");
                Log.InfoFormat("Processing {0}", webRequest.RequestUri);
                tasks.Add(ProcessRequestInternalAsync(webRequest));
            }
            var another = Task.Delay(timeout);
            tasks.Add(Task.Delay(timeout));

            var firstFinishedTask = await Task.WhenAny(tasks);

            if (firstFinishedTask == another)
            {
                throw new TimeoutException();
            }
            
                firstFinishedTask = await Task.WhenAny(tasks);
            
            return ((Task<string>)firstFinishedTask).Result;
        }
        
        //        foreach (var task in tasks.Keys)
        //            {
        //                if (!task.IsCompleted)
        //                    ProcessCancelRequestAsync(tasks[task]);
        //            }
        //        private void ProcessCancelRequestAsync(Tuple<string, string> hostnameAndQuery)
        //        {
        //            string hostname = hostnameAndQuery.Item1;
        //            string query = hostnameAndQuery.Item1;
        //            
        //            var request = WebRequest.CreateHttp(Uri.EscapeUriString($"{hostname}?abort={query}"));
        //            request.Proxy = null;
        //            request.KeepAlive = true;
        //            request.ServicePoint.UseNagleAlgorithm = false;
        //            request.ServicePoint.ConnectionLimit = 100500;
        //
        //            request.GetResponse();
        //        }

        protected override ILog Log => LogManager.GetLogger(typeof(AllClusterClient));
    }
}
