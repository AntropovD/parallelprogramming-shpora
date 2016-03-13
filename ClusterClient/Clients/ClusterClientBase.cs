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
        protected ConcurrentDictionary<string, Tuple<long, int>> ReplicaStatistics { get; set; }

        protected ClusterClientBase(string[] replicaAddresses)
        {
            ReplicaAddresses = replicaAddresses;
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

                    var key = "http://" + request.RequestUri.Authority + request.RequestUri.AbsolutePath;
                    ReplicaStatistics.AddOrUpdate(key, Tuple.Create(timer.ElapsedMilliseconds, 1),
                        (s, tuple) => Tuple.Create(tuple.Item1 + timer.ElapsedMilliseconds, tuple.Item2 + 1));
                    return result;
                }
            }
            catch (Exception ex)
            {
                Log.ErrorFormat("{0}", ex);
                throw;
            }
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

    public class GrayList
    {
        public ConcurrentDictionary<string, DateTime> list;
        public int maxGrayListSize;

        public GrayList(ConcurrentDictionary<string, DateTime> grayList, int maxSize)
        {
            list = grayList;
            maxGrayListSize = maxSize;
        }

        protected void UpdateGrayTable()
        {
            if (list.Count == 0)
                return;
            var currTime = DateTime.Now;
            list = new ConcurrentDictionary<string, DateTime>(list.Where(pair => pair.Value <= currTime));
        }
    }

    public class ReplicasEnumerable : IEnumerable
    {
        private IEnumerable<string> replicas;
        public GrayList grayList;

        public ReplicasEnumerable(IEnumerable<string> replicas, GrayList grayList)
        {
            this.replicas = replicas;
             this.grayList = grayList;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (IEnumerator)GetEnumerator();
        }

        public MyEnumerator GetEnumerator()
        {
            return new MyEnumerator(replicas, grayList);
        }
    }

    public class MyEnumerator : IEnumerator
    {
        private List<string> replicas;
        private GrayList grayList;
        private int position = -1;

        public MyEnumerator(IEnumerable<string> replicas, GrayList grayList)
        {
            this.replicas = replicas.ToList();
            this.grayList = grayList;
        }

        public bool MoveNext()
        {
            position++;
            return (position < replicas.Count);
        }

        public void Reset()
        {
            position = -1;
        }

        public object Current
        {
            get
            {
                var replica = replicas[position];
                if (grayList.ContainsKey())
                    return Current;

            }
        }
    }

   
}