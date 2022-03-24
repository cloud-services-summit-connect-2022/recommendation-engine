package org.globex.retail.streams.collectors;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

//@JsonSerialize(using = FixedSizePriorityQueueSerializer.class)
//@JsonDeserialize(using = FixedSizePriorityQueueDeserializer.class)
public class FixedSizePriorityQueue<T> {

    private final TreeSet<T> inner;

    private final int maxSize;

    public FixedSizePriorityQueue(Comparator<T> comparator, int maxSize) {
        this.inner = new TreeSet<>(comparator);
        this.maxSize = maxSize;
    }

    public FixedSizePriorityQueue(Comparator<T> comparator) {
        this.inner = new TreeSet<>(comparator);
        this.maxSize = -1;
    }

    public FixedSizePriorityQueue<T> add(T element) {
        inner.add(element);
        if (maxSize > 0 && inner.size() > maxSize) {
            inner.pollLast();
        }
        return this;
    }

    public FixedSizePriorityQueue<T> remove(T element) {
        inner.remove(element);
        return this;
    }

    public Iterator<T> iterator() {
        return inner.iterator();
    }

    public int size() {
        return inner.size();
    }
}
