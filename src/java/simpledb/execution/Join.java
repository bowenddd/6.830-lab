package simpledb.execution;

import simpledb.common.Type;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.lang.reflect.Field;
import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;


    // User defined variable

    private JoinPredicate p;

    private OpIterator child1;

    private OpIterator child2;

    private TupleDesc td;

    private boolean iteratorEND;

    private boolean child2TupleEnd;

    private Tuple child1Tuple;
    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        int field1Number = this.p.getField1();
        String field1Name = this.child1.getTupleDesc().getFieldName(field1Number);
        return field1Name;
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        int field2Number = this.p.getField2();
        String field2Name = this.child2.getTupleDesc().getFieldName(field2Number);
        return field2Name;
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td1 = this.child1.getTupleDesc();
        TupleDesc td2 = this.child2.getTupleDesc();
        int td1Nums = td1.numFields();
        int td2Nums = td2.numFields();
        Type[] types = new Type[td1Nums+td2Nums];
        String[] names = new String[td1Nums+td2Nums];
        for(int i = 0 ; i < td1Nums ; i++){
            types[i] = td1.getFieldType(i);
            names[i] = td1.getFieldName(i);
        }
        for(int i = 0 ; i < td2Nums ; i++){
            types[td1Nums+i] = td2.getFieldType(i);
            names[td1Nums+i] = td2.getFieldName(i);
        }
        return new TupleDesc(types,names);
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child1.open();
        this.child2.open();
        this.iteratorEND = false;
        this.child2TupleEnd = true;
        this.td = this.getTupleDesc();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child1.close();
        this.child2.close();
        this.iteratorEND = true;
        this.child2TupleEnd = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child1.rewind();
        this.child2.rewind();
        this.iteratorEND = false;
        this.child2TupleEnd = true;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        Tuple child1Tuple;
        while(!this.iteratorEND){
            if(this.child2TupleEnd){
                if(this.child1.hasNext()){
                    this.child1Tuple = this.child1.next();
                }else{
                    this.iteratorEND = true;
                    return null;
                }
                this.child2.rewind();
                this.child2TupleEnd = false;
            }
            while(this.child2.hasNext()){
                Tuple child2Tuple = this.child2.next();
                if(this.p.filter(this.child1Tuple, child2Tuple)){
                    return generateNewTuple(this.child1Tuple,child2Tuple,this.td);
                }
            }
            this.child2TupleEnd = true;
        }
        return null;
    }

    private static Tuple generateNewTuple(Tuple tuple1, Tuple tuple2, TupleDesc td){
        Tuple tuple = new Tuple(td);
        int tuple1Nums = tuple1.getTupleDesc().numFields();
        int tuple2Nums = tuple2.getTupleDesc().numFields();
        for(int i = 0 ; i < tuple1Nums ; i++){
            tuple.setField(i,tuple1.getField(i));
        }
        for(int i = 0 ; i < tuple2Nums ; i++){
            tuple.setField(i+tuple1Nums,tuple2.getField(i));
        }
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child1,this.child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if(children.length == 2){
            this.child1 = children[0];
            this.child2 = children[1];
        }
    }

}
