package simpledb;

import java.io.*;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    private TransactionId tid;
    private File f;
    private HeapFile heapf;
    private int pageNumCursor;
    private Iterator<Tuple> iterator;
    private HeapPage page;
    private int TableId;


    public HeapFileIterator(TransactionId tid, File file, int tableid){
        //initialize private variables
        tid = tid;
        f = file;
        TableId = tableid;
        heapf = new HeapFile(f, Database.getCatalog().getTupleDesc(TableId)); //gets thw Heap file with the passed in table id
    }
    //close iterator
    public void close(){
        iterator = null;
        page = null;
        heapf = null;


    }
    //opens the iterator
    public void open()   throws DbException, TransactionAbortedException{

        pageNumCursor = 0; //first page of the file
        //getting the first page from teh file
        page = (HeapPage)Database.getBufferPool().getPage(tid, (PageId) new HeapPageId(heapf.getId(), pageNumCursor), Permissions.READ_ONLY);
//
        iterator = page.iterator();
    }
    public boolean hasNext()throws DbException, TransactionAbortedException
    {
        //
        if(iterator == null) return false; //if iterator is null return fqlse
        if(iterator.hasNext()) return true;//if the iterator has not reached the end of the page return true
        if(pageNumCursor + 1 >=  heapf.numPages()) return false; //if all the pages have been read return false
        while(pageNumCursor + 1 <  heapf.numPages() && !iterator.hasNext()){ //this loop finds the next page with any tuples in them
            // move on to the next page
            pageNumCursor = pageNumCursor +1;
            //get the bext page and its iterator
            page = (HeapPage)Database.getBufferPool().getPage(tid, (PageId) new HeapPageId(heapf.getId(), pageNumCursor), Permissions.READ_ONLY);
            iterator = page.iterator();
        }
        return this.hasNext();

    }

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{

        if(iterator == null) //if iterator is null throws an exception
            throw new NoSuchElementException();
        return iterator.next();

//         if (iterator.hasNext()){
//              return iterator.next();
//         }
//         else {
//             throw new NoSuchElementException();
//         }

    }

    public void rewind() throws DbException, TransactionAbortedException{
        pageNumCursor =0; // go back to page number one
        //get page
        page = (HeapPage)Database.getBufferPool().getPage(tid, (PageId) new HeapPageId(heapf.getId(), pageNumCursor), Permissions.READ_ONLY);
        iterator = page.iterator();
    }

}