using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public abstract class ClusterClientBase
    {
        protected TimeSpan GrayListWaitTime => TimeSpan.FromSeconds(5.0);
        protected string[] ReplicaAddresses { get; set; }
        protected ConcurrentDictionary<string, DateTime> GrayList { get; set; }
        protected ConcurrentDictionary<string, Tuple<long, int>> ReplicaStatistics { get; set; }

        protected ClusterClientBase(string[] replicaAddresses)
        {
            ReplicaAddresses = replicaAddresses;
            GrayList = new ConcurrentDictionary<string, DateTime>();
            ReplicaStatistics = new ConcurrentDictionary<string, Tuple<long, int>>();
        }

        public abstract Task<string> ProcessRequestAsync(string query, TimeSpan timeout);
        protected abstract ILog Log { get; }
        
        protected static HttpWebRequest CreateRequest(string uriStr)
        {
            var request = WebRequest.CreateHttp(Uri.EscapeUriString(uriStr));
            request.Proxy = null;
            request.KeepAlive = true;
            request.ServicePoint.UseNagleAlgorithm = false;
            request.ServicePoint.ConnectionLimit = 100500;
            return request;
        }

        protected async Task<string> ProcessRequestInternalAsync(WebRequest request)
        {
            try
            {
                var timer = Stopwatch.StartNew();
                using (var response = await request.GetResponseAsync())
                {
                    var result = await new StreamReader(response.GetResponseStream(), Encoding.UTF8).ReadToEndAsync();
                    Log.InfoFormat("Response from {0} received in {1} ms", request.RequestUri, timer.ElapsedMilliseconds);

                    Tuple<long, int> mean;
                    var key = "http://" + request.RequestUri.Authority + request.RequestUri.AbsolutePath;

                    //ReplicaStatistics.AddOrUpdate(key, )
                    if (!ReplicaStatistics.TryGetValue(key, out mean))
                    {
                        mean = Tuple.Create(timer.ElapsedMilliseconds, 1);
                        ReplicaStatistics.TryAdd(key, mean);
                    }
                    else
                        ReplicaStatistics.TryUpdate(key, ReplicaStatistics[key],
                            Tuple.Create(mean.Item1 + timer.ElapsedMilliseconds, mean.Item2 + 1));

                    return result;
                }
            }
            catch (Exception ex)
            {
                Log.ErrorFormat("{0}", ex);
                throw;
            }
        }

        protected void UpdateGrayTable()
        {
            if (GrayList.Count == 0)
                return;
            var currTime = DateTime.Now;
            GrayList = new ConcurrentDictionary<string, DateTime>(GrayList.Where(pair => pair.Value <= currTime));
        }

        protected IEnumerable<string> GetRandomReplicaSequence()
        {
            var rnd = new Random();
            return Enumerable.Range(0, ReplicaAddresses.Length)
                .OrderBy(i => rnd.Next(ReplicaAddresses.Length))
                .Select(i => ReplicaAddresses[i]);
        }

        protected IEnumerable<string> GetStatisticsReplicaSequence()
        {
            if (ReplicaStatistics.Count == ReplicaAddresses.Length)
                return ReplicaStatistics.OrderBy(pair => pair.Value.Item1*1.0 / pair.Value.Item2)
                    .Select(pair => pair.Key);
            return GetRandomReplicaSequence();
        }
    }
}