package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;


    // User defined member variable

    private Map<PageId,LockManager> pageLockManagerMap;

    private Map<TransactionId,Set<PageId>> txPageLockMap;

    private int numPages;

    private int pageCount;

    private Map<PageId,Node<Page>> pagesMap;

    private Map<TransactionId,Set<PageId>> dirtyPageIdMap;

    private LinkedList<Page> list;

    private Map<PageId,Page> dirtyPageMap;



    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = DEFAULT_PAGES;
        if (numPages >= 0){
            this.numPages = numPages;
        }
        this.pageLockManagerMap =  new HashMap<>();
        this.pageCount = 0;
        this.pagesMap = new HashMap<>();
        this.dirtyPageMap = new HashMap<>();
        this.list = new LinkedList<>(null,null,0,numPages);
        this.dirtyPageIdMap = new HashMap<>();
        this.txPageLockMap = new HashMap<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException, IllegalArgumentException{
        // some code goes here
        if(tid == null){
            throw new TransactionAbortedException();
        }
        // 从pageLockMap中根据pageId获得对应的锁，如果没有就new一个新的锁
        LockManager manager = this.pageLockManagerMap.get(pid);
        if(manager == null){
            manager = new LockManager();
            this.pageLockManagerMap.put(pid,manager);
        }
        // 获得了page之后需要根据请求中的权限加相应的读写锁
        if(perm.equals(Permissions.READ_ONLY)){
            if(!(tid.equals(manager.getTid()) && (Permissions.READ_WRITE.equals(manager.getPerm())))){
                manager.readLock();
            }

        }else if(perm.equals(Permissions.READ_WRITE)){
            if(tid.equals(manager.getTid()) && (Permissions.READ_ONLY.equals(manager.getPerm()))){
                manager.lockUpgrade();
            }else{
                manager.writeLock();
            }

        }
        manager.setPermAndTransactionId(perm,tid);
        // 先查找bufferPool中是否有所需的page，如果有直接返回，
        // 否则从disk导入page，并将page导入bufferPool中
        Page page = this.getPageFromBufferPool(pid);
        if (page == null){
            page = this.getPageFromDisk(pid);
        }
        Set<PageId> pageIds = this.txPageLockMap.getOrDefault(tid, new HashSet<>());
        pageIds.add(pid);
        this.txPageLockMap.put(tid,pageIds);
        return page;
    }


    private synchronized Page getPageFromBufferPool(PageId pageId) {
        Node<Page> node = this.pagesMap.get(pageId);
        if(node == null){
            return null;
        }
        this.list.remove(node);
        this.list.add(node);
        return node.getData();
    }

    private Page getPageFromDisk(PageId pageId)
            throws TransactionAbortedException, DbException, IllegalArgumentException{
        // 根据pageId，拿到tableId，然后再去找对应的page
        Catalog catalog = Database.getCatalog();
        int tableId = pageId.getTableId();
        Page page = catalog.getDatabaseFile(tableId).readPage(pageId);
        if(page != null){
            loadPageToBufferPool(pageId,page);
            return page;
        }
        throw new TransactionAbortedException();
    }

    private synchronized void loadPageToBufferPool(PageId pageId,Page page) throws DbException {
        if(this.pageCount == this.numPages){
            this.evictPage();
        }
        Node node = new Node(page,null,null);
        this.list.add(node);
        this.pagesMap.put(pageId,node);
        ++this.pageCount;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> pageIds = this.txPageLockMap.get(tid);
        if(pageIds == null || !pageIds.contains(pid)){
            return;
        }
        LockManager manager = this.pageLockManagerMap.get(pid);
        manager.unlock();
        manager.setPermAndTransactionId(null,null);
        pageIds.remove(pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,false);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> pageIds = this.txPageLockMap.get(tid);
        if(pageIds == null){
            return false;
        }
        return pageIds.contains(p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> pageIds = this.txPageLockMap.get(tid);
        for(PageId pageId:pageIds){
            LockManager manager = this.pageLockManagerMap.get(pageId);
            manager.unlock();
            manager.setPermAndTransactionId(null,null);
        }
        pageIds.clear();
        this.txPageLockMap.remove(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = file.insertTuple(tid, t);
        for(Page page :pages){
            page.markDirty(true,tid);
            this.addOrUpdatePage(page);
            Set<PageId> pageIdSet = this.dirtyPageIdMap.getOrDefault(tid, new HashSet<>());
            pageIdSet.add(page.getId());
            this.dirtyPageIdMap.put(tid,pageIdSet);
            this.dirtyPageMap.put(page.getId(),page);
        }
    }


    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = file.deleteTuple(tid, t);
        for(Page page : pages){
            page.markDirty(true,tid);
            this.addOrUpdatePage(page);
            Set<PageId> pageIdSet = this.dirtyPageIdMap.getOrDefault(tid, new HashSet<>());
            pageIdSet.add(page.getId());
            this.dirtyPageIdMap.put(tid,pageIdSet);
            this.dirtyPageMap.put(page.getId(),page);
        }
    }

    private void addOrUpdatePage(Page page) throws DbException, IOException {
        Node<Page> node = this.pagesMap.get(page.getId());
        if(node == null){
            this.loadPageToBufferPool(page.getId(),page);
            return;
        }
        node.setData(page);
        this.list.remove(node);
        this.list.add(node);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Set<PageId> set = this.dirtyPageMap.keySet();
        for(PageId pid : set){
            this.flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        Node<Page> pageNode = this.pagesMap.get(pid);
        if(pageNode != null){
            this.list.remove(pageNode);
        }
        this.dirtyPageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        int tableId = pid.getTableId();
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        Page page = this.dirtyPageMap.get(pid);
        if(page != null){
            file.writePage(page);
        }
        this.dirtyPageMap.remove(pid);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException{
        // some code goes here
        // not necessary for lab1
        Node<Page> node = this.list.back;
        this.list.remove(node);
        this.pagesMap.remove(node.getData().getId());
        --this.pageCount;
        try {
            this.flushPage(node.getData().getId());
        }catch (IOException e){
            throw new DbException("write page to disk error");
        }
    }

}

class LinkedList <T> {
    Node<T> front;
    Node<T> back;
    int length;
    int size;

    public LinkedList(Node<T> front, Node<T> back, int length, int size) {
        this.front = front;
        this.back = back;
        this.length = length;
        this.size = size;
    }

    public Node<T> getFront() {
        return front;
    }

    public Node<T> getBack() {
        return back;
    }

    public int getLength() {
        return length;
    }

    public void add(Node<T> node){
        node.next = front;
        node.prev = null;
        if(this.front == null){
            this.front = node;
            this.back = node;
        }
        this.length++;
    }

    public void remove(Node<T> node){
        if(this.front == this.back){
            this.front = null;
            this.back = null;
            this.length = 0;
            return;
        }
        if(node.equals(this.front)){
            this.front = node.next;
        }else if(node.equals(this.back)){
            this.back = node.prev;
            this.back.next = null;
        }else{
            node.prev.next = node.next;
        }
        this.length--;
    }
}

class Node<T>{
    T data;
    Node<T> next;
    Node<T> prev;

    public Node(T data, Node<T> next, Node<T> prev) {
        this.data = data;
        this.next = next;
        this.prev = prev;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}

class LockManager{

    private ReentrantReadWriteLock rwLock;

    private Permissions perm;

    private TransactionId tid;

    public LockManager() {
        this.rwLock = new ReentrantReadWriteLock();
    }

    public void readLock(){
        this.rwLock.readLock().lock();
    }

    public void writeLock(){
        this.rwLock.writeLock().lock();
    }

    public void setPermAndTransactionId(Permissions perm, TransactionId tid){
        this.perm = perm;
        this.tid = tid;
    }

    public TransactionId getTid(){
        return this.tid;
    }

    public Permissions getPerm(){
        return this.perm;
    }

    public void unlock(){
        if(Permissions.READ_ONLY.equals(this.perm)){
            this.rwLock.readLock().unlock();
        }else if(Permissions.READ_WRITE.equals(this.perm)){
            this.rwLock.writeLock().unlock();
        }
    }

    public void lockUpgrade(){
        this.perm = Permissions.READ_WRITE;
        this.rwLock = new ReentrantReadWriteLock();
        this.rwLock.writeLock();
    }
}