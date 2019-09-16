package org.apache.livy.thriftserver.session;

import java.util.Iterator;

/**
 * Wrapper that provides a Java iterator interface over a Scala iterator.
 */
class ScalaRDDStreamIterator<T> implements Iterator<T> {

    private final RDDStreamIterator<T> it;

    ScalaRDDStreamIterator(RDDStreamIterator<T> it) {
        this.it = it;
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public T next() {
        return it.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
