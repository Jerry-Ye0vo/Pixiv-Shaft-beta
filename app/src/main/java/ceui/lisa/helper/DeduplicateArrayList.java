package ceui.lisa.helper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import ceui.lisa.models.Deduplicatable;

/**
 * 自动去重的 ArrayList
 * @param <E>
 */
public class DeduplicateArrayList<E extends Deduplicatable> extends ArrayList<E> {

    private Set<Object> innerSet = null;

    public DeduplicateArrayList(int initialCapacity) {
        super(initialCapacity);
        innerSet = new HashSet<>();
    }

    public DeduplicateArrayList() {
        innerSet = new HashSet<>();
    }

    public DeduplicateArrayList(@NonNull Collection<? extends E> c) {
        this(c.size());
        addAll(c);
    }

    @Override
    public boolean add(E e) {
        return innerSet.add(e.getDuplicateKey()) && super.add(e);
    }

    @Override
    public void add(int index, E element) {
        checkPosition(index);
        if (innerSet.add(element.getDuplicateKey())) super.add(index, element);
    }

    @Override
    public boolean addAll(@NonNull Collection<? extends E> c) {
        return addAll(size(), c);
    }

    @Override
    public boolean addAll(int index, @NonNull Collection<? extends E> c) {
        checkPosition(index);
        List<E> fresh = new ArrayList<>(c.size());
        for (E item : c) {
            if (innerSet.add(item.getDuplicateKey())) fresh.add(item);
        }
        return super.addAll(index, fresh);
    }

    private void checkPosition(int index) {
        if (index < 0 || index > size()) throw new IndexOutOfBoundsException("index=" + index);
    }

    private void rebuildKeys() {
        innerSet.clear();
        for (E item : this) innerSet.add(item.getDuplicateKey());
    }

    @Override public E set(int index, E element) {
        E old = get(index);
        Object key = element.getDuplicateKey();
        if (!java.util.Objects.equals(old.getDuplicateKey(), key) && innerSet.contains(key)) {
            throw new IllegalArgumentException("Duplicate key: " + key);
        }
        E result = super.set(index, element);
        innerSet.remove(old.getDuplicateKey());
        innerSet.add(key);
        return result;
    }

    @Override public boolean removeAll(@NonNull Collection<?> c) {
        try { return super.removeAll(c); } finally { rebuildKeys(); }
    }

    @Override public boolean retainAll(@NonNull Collection<?> c) {
        try { return super.retainAll(c); } finally { rebuildKeys(); }
    }

    @Override public boolean removeIf(Predicate<? super E> filter) {
        try { return super.removeIf(filter); } finally { rebuildKeys(); }
    }

    @Override protected void removeRange(int fromIndex, int toIndex) {
        super.removeRange(fromIndex, toIndex);
        rebuildKeys();
    }

    @Override public void replaceAll(UnaryOperator<E> operator) {
        List<E> replacement = new ArrayList<>(size());
        Set<Object> keys = new HashSet<>();
        for (E item : this) {
            E next = operator.apply(item);
            if (!keys.add(next.getDuplicateKey())) throw new IllegalArgumentException("Duplicate replacement key");
            replacement.add(next);
        }
        super.clear();
        super.addAll(replacement);
        innerSet = keys;
    }

    @Override public Object clone() {
        return new DeduplicateArrayList<>(this);
    }

    @Override
    public boolean remove(@Nullable @org.jetbrains.annotations.Nullable Object o) {
        boolean removed = super.remove(o);
        if (removed && o instanceof Deduplicatable) innerSet.remove(((Deduplicatable) o).getDuplicateKey());
        return removed;
    }

    @Override
    public E remove(int index) {
        E e = super.remove(index);
        Object obj = e.getDuplicateKey();
        innerSet.remove(obj);
        return e;
    }

    @Override
    public void clear() {
        super.clear();
        innerSet.clear();
    }

    /**
     *
     * @param dist 目标集合
     * @param src 数据来源集合
     * @param <T> Deduplicatable
     */
    public static <T extends Deduplicatable> void addAllWithNoRepeat(Collection<T> dist, Collection<T> src) {
        final Set<Object> set = new HashSet<>();
        for (T element : dist) {
            set.add(element.getDuplicateKey());
        }
        for (T element : src) {
            if (set.add(element.getDuplicateKey())) {
                dist.add(element);
            }
        }
    }
}
