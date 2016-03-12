using System;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using log4net;
using log4net.Config;

namespace ClusterServer
{
    public static class Program
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof (Program));

        public static void Main(string[] args)
        {
            XmlConfigurator.Configure();

            try
            {
                ServerArguments parsedArguments;
                if (!ServerArguments.TryGetArguments(args, out parsedArguments))
                    return;

                var listener = new HttpListener
                               {
                                   Prefixes =
                                   {
                                       string.Format("http://+:{0}/{1}/",
                                           parsedArguments.Port,
                                           parsedArguments.MethodName)
                                   }
                               };

                listener.StartProcessingRequests(CreateCallback(parsedArguments));
                while (true)
                {
                    if (Console.KeyAvailable)
                        if (Console.ReadKey(true).Key == ConsoleKey.Escape)
                            break;
                }
                Console.WriteLine("Server finished");
            }
            catch (Exception e)
            {
                Log.Fatal(e);
            }
        }

        private static Action<HttpListenerContext> CreateCallback(ServerArguments parsedArguments)
        {
            return context =>
                   {
                       var currentRequestId = Interlocked.Increment(ref RequestId);
                       Console.WriteLine("Thread #{0} received request #{1} at {2}",
                           Thread.CurrentThread.ManagedThreadId, currentRequestId, DateTime.Now.TimeOfDay);

                       Thread.Sleep(parsedArguments.MethodDuration);

                       var encryptedBytes = GetBase64HashBytes(context.Request.QueryString["query"], Encoding.UTF8);
                       await context.Response.OutputStream.Write(encryptedBytes, 0, encryptedBytes.Length);

                       Console.WriteLine("Thread #{0} sent response #{1} at {2}",
                           Thread.CurrentThread.ManagedThreadId, currentRequestId,
                           DateTime.Now.TimeOfDay);
                   };
        }

        private static byte[] GetBase64HashBytes(string query, Encoding encoding)
        {
            using (var hasher = new HMACMD5(Key))
            {
                var hash = Convert.ToBase64String(hasher.ComputeHash(encoding.GetBytes(query)));
                return encoding.GetBytes(hash);
            }
        }

        private static readonly byte[] Key = Encoding.UTF8.GetBytes("Контур.Шпора");
        private static int RequestId;
    }
}
