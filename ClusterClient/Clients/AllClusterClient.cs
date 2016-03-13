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
            var timeoutTask = Task.Delay(timeout);
            tasks.Add(Task.Delay(timeout));

            while (true)
            {
                var finishedTask = await Task.WhenAny(tasks);
                if (finishedTask == timeoutTask)
                {
                    throw new TimeoutException();
//                     тут бы хорошо сделать подобное:
//                    foreach (var task in tasks)
//                    {
//                        if (task.Status != TaskStatus.RanToCompletion)
//                            grayList.Add(task.Request);
//                    }
//                    но из таска нельзя вытащить адрес реквеста, по которому обращались, чтоб передать в серый список
                }
                if (finishedTask.Status == TaskStatus.RanToCompletion)
                    return ((Task<string>) finishedTask).Result;
            }
        }
        protected override ILog Log => LogManager.GetLogger(typeof(AllClusterClient));
    }
}
