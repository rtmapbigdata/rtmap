package cn.rtmap.bigdata.ingest.utils;

import java.util.LinkedList;

public class Queue<T>
{
    protected LinkedList<T> list;

    public Queue() {
        list = new LinkedList<T>();
    }

    public void add( T element) {
        list.add( element);
    }

    public T removeLast() {
    	if (list.size() > 0)
    		return list.removeLast();
    	else
    		return null;
    }

    public int count() {
    	return list.size();
    }
}