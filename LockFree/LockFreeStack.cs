using System.Threading;

namespace LockFree
{
    public class LockFreeStack<T> : IStack<T>
    {
        private Node<T> head;

        public void Push(T obj)
        {
            Node<T> oldHead;
            var newHead = new Node<T> {Value = obj};

            do
            {
                oldHead = head;
                newHead.Next = oldHead;
            } while (Interlocked.CompareExchange(ref head, newHead, oldHead) != oldHead);
        }

        public T Pop()
        {
            Node<T> oldHead, newHead;

            do
            {
                oldHead = head;
                newHead = oldHead.Next;
            } while (Interlocked.CompareExchange(ref head, newHead, oldHead) != oldHead);

            return oldHead.Value;
        }
    }
}