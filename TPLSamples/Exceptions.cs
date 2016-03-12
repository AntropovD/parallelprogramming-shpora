using System;
using System.Threading;
using System.Threading.Tasks;

namespace TPLSamples
{
    public static class Exceptions
    {
        public static void WaitAndStatus()
        {
            var crashingTask = Task.Run(() =>
                                        {
                                            throw new Exception("haha!");
                                        });

            try
            {
                crashingTask.Wait();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            Console.WriteLine("CrashingTask status is {0}", crashingTask.Status);
        }

        public static void ContinueWith()
        {
            var crashingTask = Task.Run(() =>
                                        {
                                            throw new Exception("haha!");
                                        });
            var continuationTask =
                crashingTask.ContinueWith(t => Console.WriteLine("CrashingTask status is {0}", t.Status)/*, TaskContinuationOptions.OnlyOnRanToCompletion*/);

            continuationTask.Wait();

            Console.WriteLine("ContinuationTask status is {0}", continuationTask.Status);
        }

        public static void Handling()
        {
            var firstCrashingTask = Task.Run(() =>
                                             {
                                                 Thread.Sleep(1000);
                                                 throw new ArgumentException();
                                             });
            var secondCrashingTask = Task.Run(() => { throw new TimeoutException(); });
            var aggregationTask = Task.WhenAll(firstCrashingTask, secondCrashingTask);

            aggregationTask.ContinueWith(_ => { }).Wait();

            aggregationTask.Exception.Handle(e =>
                                             {
                                                 Console.WriteLine(e.GetType());
                                                 return true;
                                             });
        }

        public static void Flattening()
        {
            var crashingTask = Task.Factory.StartNew((() =>
                                                      {
                                                          var childCrashingTask =
                                                              Task.Factory.StartNew(() => { throw new TimeoutException(); },
                                                                  TaskCreationOptions.AttachedToParent);

                                                          throw new ArgumentException();
                                                      }));

            crashingTask.ContinueWith(_ => { }).Wait();

            crashingTask.Exception/*.Flatten()*/.Handle(e =>
                                                        {
                                                            Console.WriteLine(e.GetType());
                                                            return true;
                                                        });
        }
    }
}