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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private Map<PageId, ReentrantReadWriteLock> pageRWLock;

    private Lock tableLock;

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
        this.pageRWLock = new HashMap<>();
        this.tableLock = new ReentrantLock();
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

    private ReentrantReadWriteLock getPageRWLock(PageId pid){
        ReentrantReadWriteLock lock =
                this.pageRWLock.get(pid);
        if(lock == null){
            lock = new ReentrantReadWriteLock();
            this.pageRWLock.put(pid,lock);
        }
        return lock;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        if(pid.getTableId() != this.getId()){
            throw new IllegalArgumentException();
        }
        ReentrantReadWriteLock lock = this.getPageRWLock(pid);
        int pageSize = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * pageSize;
        if(this.getFile().length() < offset + pageSize){
            throw new IllegalArgumentException();
        }
        byte[] data = new byte[pageSize];
        RandomAccessFile raf = null;
        try {
            lock.readLock().lock();
            raf = new RandomAccessFile(this.getFile(),"r");
            raf.seek(offset);
            raf.read(data);
            return new HeapPage(new HeapPageId(pid.getTableId(),pid.getPageNumber()),data);
        } catch (Exception ignored) {
            ignored.printStackTrace();
            throw new IllegalArgumentException();
        }finally {
            if (raf != null){
                try {
                    raf.close();
                }catch (IOException ignored){

                }
            }
            lock.readLock().unlock();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        if(pid.getTableId() != this.getId()){
            throw new IllegalArgumentException();
        }
        int offset = pid.getPageNumber()*BufferPool.getPageSize();
        ReentrantReadWriteLock lock = this.getPageRWLock(pid);
        byte[] data = page.getPageData();
        RandomAccessFile raf = null;
        try {
            lock.writeLock().lock();
            raf = new RandomAccessFile(this.getFile(),"rw");
            raf.seek(offset);
            raf.write(data);
        } catch (Exception ignored) {
            ignored.printStackTrace();
            throw new IllegalArgumentException();
        }finally {
            if(raf != null){
                raf.close();
            }
            lock.writeLock().unlock();
        }
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
        if(!t.getTupleDesc().equals(this.getTupleDesc())){
            throw new DbException("tuple desc not match when inserting tuple");
        }
        boolean inserted = false;
        HeapPage page = null;
        while(!inserted){
            PageId pid = this.findEmptyPage(tid);
            if(pid == null){
                // 新建一个新的page
                this.tableLock.lock();
                pid = this.findEmptyPage(tid);
                if (pid == null){
                    pid = new HeapPageId(this.getId(),this.numPages());
                    page = new HeapPage((HeapPageId) pid,HeapPage.createEmptyPageData());
                    this.writePage(page);
                }
                this.tableLock.unlock();
            }
            // TODO: 此处权限应该是READ_WRITE,由于BuffPool未完全实现，故暂设为READ_ONLY
            page = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
            if(page.getNumEmptySlots()==0){
                continue;
            }
            page.insertTuple(t);
            inserted = true;
        }
        return new ArrayList<>(Arrays.asList((Page) page));
        // not necessary for lab1
    }

    private PageId findEmptyPage(TransactionId tid) throws TransactionAbortedException, DbException, IOException {
        for(int i = 0 ; i < this.numPages() ; i++){
            PageId pid = new HeapPageId(this.getId(),i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
            if(page.getNumEmptySlots()!=0){
                return pid;
            }
        }
        return null;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, IOException {
        // some code goes here
        // TODO: 此处权限应该是READ_WRITE,由于BuffPool未完全实现，故暂设为READ_ONLY
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_ONLY);
        page.deleteTuple(t);
        return new ArrayList<>(Arrays.asList((Page) page));
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
    public void open() throws DbException, TransactionAbortedException{
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
        while((it == null || !it.hasNext())&& this.pageNo < this.f.numPages() - 1){

            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(this.tid, new HeapPageId(this.f.getId(), ++this.pageNo), Permissions.READ_ONLY);
            it = heapPage.iterator();
        }
        if (it == null || !it.hasNext()){
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

