using System;
using System.Threading;
using System.Threading.Tasks;

namespace TPLSamples
{
    public static class Continuation
    {
        public static void Parent()
        {
            var parent = Task.Factory
                .StartNew(() =>
                          {
                              Console.WriteLine("Outer task executing.");
                              Task.Factory.StartNew(() =>
                                                    {
                                                        Console.WriteLine("Nested task executing.");
                                                        Thread.SpinWait(500000);
                                                        Console.WriteLine("Nested task completing.");
                                                    }, TaskCreationOptions.AttachedToParent);
                          } /*, TaskCreationOptions.DenyChildAttach*/);

            parent.Wait();
            Console.WriteLine("Outer has completed.");
        }
        
        public static void ContinueWith()
        {
            var tasksChain = Task.Run(
                () => Console.WriteLine("Starting in thread #{0}", Thread.CurrentThread.ManagedThreadId))
                .ContinueWith(previousTask =>
                              {
                                  Thread.Sleep(1000);
                                  Console.WriteLine("Slept in thread #{0}", Thread.CurrentThread.ManagedThreadId);
                              })
                .ContinueWith(previousTask =>
                    Console.WriteLine("SleepingTask status is {0}", previousTask.Status)/*,
                    TaskContinuationOptions.OnlyOnRanToCompletion*/);
                                 
            tasksChain.Wait();
        }
        
        public static void TaskStatusWhenContinueWith()
        {
            var task = Task.Run(() =>
                                {
                                    Console.WriteLine("Sleping in thread #{0}", Thread.CurrentThread.ManagedThreadId);
                                    Thread.Sleep(1000);
                                });

            var continuationTask = task.ContinueWith(previousTask => Console.WriteLine("Finished sleeping"));

            Console.WriteLine("ContinuationTask status is {0}", continuationTask.Status);
        }
    }
}