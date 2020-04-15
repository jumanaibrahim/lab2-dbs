package simpledb;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        this.numPages = (int) (file.length()/BufferPool.getPageSize());

    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return(file);
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return(file.getAbsoluteFile().hashCode());
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return(td);
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
//        System.out.println("PID: " + pid.getPageNumber());
//        System.out.println("PID - numpages: " + numPages());
        if(pid.getPageNumber() < 0 || pid.getPageNumber() >= this.numPages()){
            throw new IllegalArgumentException("Page number does not exist in this file");
        }

        int begin = pid.getPageNumber() * BufferPool.getPageSize();
        byte[] pageData = new byte[BufferPool.getPageSize()];

        //Try reading in the heap page information.
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r"); //Open file for reading only
            raf.seek(begin);
            raf.readFully(pageData); //Blocks and reads exactly pageData of bytes;
            raf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }

        //Try creating the HeapPage from our read data.
        try {
            HeapPage n1 = new HeapPage((HeapPageId) pid, pageData);
            return(n1);
        } catch (IOException e){
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {

        try {
            PageId pid= page.getId();
            HeapPageId hpid= (HeapPageId)pid;

            RandomAccessFile rAf=new RandomAccessFile(file,"rw");
            int offset = pid.getPageNumber()*BufferPool.getPageSize();
            byte[] b=new byte[BufferPool.getPageSize()];
            b=page.getPageData();
            rAf.seek(offset);
            rAf.write(b, 0, BufferPool.getPageSize());
            rAf.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        // some code goes here
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     * file.length() = length in bytes.
     */
    public int numPages() {

        return (int) (file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HeapPage pageCur;
//        HeapPage pageCurWrite;
        ArrayList<Page> retPages = new ArrayList<Page>();

        HeapPageId pid;
        int tableId = getId();
        //for each page in teh file

        for (int i = 0; i < numPages(); i++){

            pid = new HeapPageId(tableId,i); //get the  pageid
            pageCur = (HeapPage) Database.getBufferPool().getPage(tid,pid, Permissions.READ_WRITE); //getpage for reading

            System.out.println("Empty slots available: " + pageCur.getNumEmptySlots() + " Page Number: " + pageCur.getId().getPageNumber());

            if (pageCur.getNumEmptySlots() > 0){ //check is the  page has an empty slots
                System.out.println("Empty page found!");
//                pageCurWrite = (HeapPage) Database.getBufferPool().getPage(tid,pid, Permissions.READ_WRITE); //write to page
                pageCur.insertTuple(t);
                retPages.add(pageCur); //add the affected page to list
                return retPages;

            }

        }

        System.out.println("Creating new page: " + numPages());
//        numPages++;

        HeapPageId newPid = new HeapPageId(tableId, numPages());
        HeapPage createNew = new HeapPage(newPid, HeapPage.createEmptyPageData());


        writePage(createNew);
        HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
        newPage.insertTuple(t);
        System.out.println("llEmpty slots available: " + newPage.getNumEmptySlots() + " Page Number: " + createNew.getId().getPageNumber());

        retPages.add(newPage);

        return retPages;

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> pages = new ArrayList<Page>();

        try {
            RecordId rid = t.getRecordId();
            HeapPageId pid = (HeapPageId) rid.getPageId();
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            page.deleteTuple(t);
            pages.add(page);


        }
        catch(DbException e){
            throw e ;
        }
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
//        HeapFileIterator it = new HeapFileIterator(this, tid);
        return new HeapFileIterator(tid, file, this.getId()); // look at the class
//        return new HeapFileIterator(this, tid);

    }

}
