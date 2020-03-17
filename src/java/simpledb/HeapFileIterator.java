package simpledb;

import java.nio.Buffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator{

    TransactionId tid;
    int currPageno;
    int maxPageno;
    HeapFile hf;
    HeapPage hp;
    Iterator<Tuple> hpIt;
    boolean opened;

    public HeapFileIterator(HeapFile hf, TransactionId tid){
        this.tid = tid;
        this.hf = hf;
        this.maxPageno = hf.numPages();
        this.opened = false;

        this.currPageno = 0;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        //Move and create iterator to beginning.
        HeapPageId pid = new HeapPageId(hf.getId(), this.currPageno);
        this.hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);;
        this.hpIt = hp.iterator();
        this.opened = true;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!this.opened){
            return false;
        }

        int tempPageno = this.currPageno;
        if(this.hpIt.hasNext()){ //This page has more tuples
            return true;
        }
        tempPageno ++;
        while(tempPageno < maxPageno){ //Check next page if it has more.
            HeapPageId pid = new HeapPageId(hf.getId(),tempPageno);
            HeapPage tempHp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            Iterator<Tuple> tempIt = tempHp.iterator();
            if(tempIt.hasNext()){
                return true;
            }
            tempPageno ++;
        }
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!this.opened){
            throw new NoSuchElementException("Iterator not opened");
        }
        while (true){
            Tuple tuple = this.hpIt.next();
            if(tuple != null){
                return tuple;
            }
            this.currPageno ++;
            if(this.currPageno >= this.maxPageno){
                return null;
            }
            HeapPageId pid = new HeapPageId(hf.getId(), this.currPageno);
            this.hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if(this.hp == null){
                return null;
            }
            this.hpIt = this.hp.iterator();
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.currPageno = 0;
        this.open();
    }

    @Override
    public void close() {
        this.opened = false;
    }
}
