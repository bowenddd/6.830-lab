package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.index.BTreeFile;
import simpledb.index.BTreeLeafPage;
import simpledb.index.BTreePageId;
import simpledb.index.BTreeRootPtrPage;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    // User defined variable

    private File backedFile;

    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.backedFile = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.backedFile;
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
        // some code goes here
        int tableId = this.getFile().getAbsoluteFile().hashCode();
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException{
        // some code goes here
        if(pid.getTableId() != this.getId()){
            throw new IllegalArgumentException();
        }
        int pageSize = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * pageSize;
        byte[] data = new byte[pageSize];
        try {
            FileInputStream fin = new FileInputStream(this.getFile());
            fin.skip(offset);
            fin.read(data);
            return new HeapPage(new HeapPageId(pid.getTableId(),pid.getPageNumber()),data);
        } catch (Exception ignored) {
            ignored.printStackTrace();
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        return (int)(this.getFile().length()/pageSize);
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }

}

class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    private int pageNo;

    final TransactionId tid;
    final HeapFile f;

    /**
     * Constructor for this iterator
     * @param f - the BTreeFile containing the tuples
     * @param tid - the transaction id
     */
    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
        this.pageNo = this.f.numPages();
    }

    /**
     * Open this iterator by getting an iterator on the first heap page
     */
    public void open() throws DbException, TransactionAbortedException {
        this.pageNo = 0;
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(this.tid, new HeapPageId(this.f.getId(), this.pageNo), Permissions.READ_ONLY);
        it = heapPage.iterator();
    }

    /**
     * Read the next tuple either from the current page if it has more tuples or
     * from the next page by following the right sibling pointer.
     *
     * @return the next tuple, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext()){
            it = null;
        }
        if (it == null && this.pageNo < this.f.numPages() - 1){
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(this.tid, new HeapPageId(this.f.getId(), ++this.pageNo), Permissions.READ_ONLY);
            it = heapPage.iterator();
        }
        if (it == null){
            return null;
        }
        return it.next();
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * close the iterator
     */
    public void close() {
        super.close();
        it = null;
        pageNo = this.f.numPages();
    }
}

