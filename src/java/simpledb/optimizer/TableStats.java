package simpledb.optimizer;

import com.sun.org.apache.xml.internal.security.utils.resolver.implementations.ResolverXPointer;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    // User defined variable

    private int tableId;

    private int ioCostPrePage;

    private int numPages;

    private int totalTups;

    private Map<Integer,IntHistogram> intFieldHistMap;

    private Map<Integer,StringHistogram> strFieldHistMap;

    private TupleDesc td;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPrePage = ioCostPerPage;
        this.intFieldHistMap = new HashMap<>();
        this.strFieldHistMap = new HashMap<>();
        try {
            this.execStats();
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        }

    }

    private void execStats() throws TransactionAbortedException, DbException {
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(this.tableId);
        this.numPages = file.numPages();
        DbFileIterator iterator = file.iterator(new TransactionId());
        this.totalTups = 0;
        iterator.open();
        this.td = file.getTupleDesc();
        for(int i = 0 ; i < td.numFields() ; i++){
            Type t = td.getFieldType(i);
            if(t.equals(Type.INT_TYPE)){
                this.intFieldHistMap.put(i,new IntHistogram(10000,0,Integer.MAX_VALUE-10));
            }else{
                this.strFieldHistMap.put(i,new StringHistogram(100));
            }
        }
        while(iterator.hasNext()){
            Tuple tuple = iterator.next();
            for(int i = 0 ; i < td.numFields() ; i++){
                Type t = td.getFieldType(i);
                if(t.equals(Type.INT_TYPE)){
                    IntHistogram intHistogram = this.intFieldHistMap.get(i);
                    intHistogram.addValue(((IntField)tuple.getField(i)).getValue());
                }else{
                    StringHistogram stringHistogram = this.strFieldHistMap.get(i);
                    stringHistogram.addValue(tuple.getField(i).toString());
                }
            }
            this.totalTups++;
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.numPages*this.ioCostPrePage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (this.totalTups*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        Type type = this.td.getFieldType(field);
        if(type.equals(Type.INT_TYPE)){
            IntHistogram intHistogram = this.intFieldHistMap.get(field);
            return intHistogram.estimateSelectivity(op,0);
        }
        StringHistogram stringHistogram = this.strFieldHistMap.get(field);
        return stringHistogram.estimateSelectivity(op,"mmmm");
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type type = this.td.getFieldType(field);
        if(type.equals(Type.INT_TYPE)){
            IntHistogram intHistogram = this.intFieldHistMap.get(field);
            return intHistogram.estimateSelectivity(op,((IntField)constant).getValue());
        }
        StringHistogram stringHistogram = this.strFieldHistMap.get(field);
        return stringHistogram.estimateSelectivity(op,constant.toString());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.totalTups;
    }

}
