namespace T28.V2.Contracts;

/// <summary>Fixed live-edge queue; it intentionally has no replay/readback API.</summary>
public sealed class LiveQueue<T>
{
    private readonly Queue<T> _items = new();
    private readonly object _gate = new();
    public const int Capacity = 5;

    public int Count { get { lock (_gate) return _items.Count; } }

    public void Push(T item)
    {
        lock (_gate)
        {
            if (_items.Count == Capacity) _items.Dequeue();
            _items.Enqueue(item);
        }
    }

    public bool TryTake(out T? item)
    {
        lock (_gate) return _items.TryDequeue(out item);
    }

    public void Clear()
    {
        lock (_gate) _items.Clear();
    }
}
