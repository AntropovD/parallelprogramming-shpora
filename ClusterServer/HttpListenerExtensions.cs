using System;
using System.Net;
using System.Threading.Tasks;
using log4net;

namespace ClusterServer
{
    public static class HttpListenerExtensions
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(HttpListenerExtensions));
        
        public static async void StartProcessingRequests(this HttpListener listener, Action<HttpListenerContext> callback)
        {
            listener.Start();
            Console.WriteLine("Server started listening prefixes: {0}", string.Join(";", listener.Prefixes));

            while (true)
            {
                try
                {
                    var context = await listener.GetContextAsync();

                    try
                    {
                        Task.Factory.StartNew(() => callback(context));
                    }
                    catch (Exception e)
                    {
                        Log.Error(e);
                    }
                    finally
                    {
                        context.Response.Close();
                    }
                }
                catch (Exception e)
                {
                    Log.Error(e);
                }
            }
        }
    }
}