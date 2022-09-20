package simpledb.optimizer;

import simpledb.execution.Operator;
import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    // User defined variable
    /**
     * 说一下之前的想法，之前的想法是由于 max 和 min 之间的差与buckets不能整除，
     * 导致每个桶的左右边界可能不为整数，因此设置一个 bucketRange数组，它的长度
     * 为桶的个数，记录每一个桶的有边界。如果只设置这一个桶的话，在查找一个值应该
     * 加入到哪个桶的时候，需要线性扫描整个bucketRange数组，时间复杂度为O(n)这里
     * n为桶的个数，当桶的数量很大时，需要消耗大量的时间。
     * 有没有一种方法可以将时间复杂度优化成O(1)?
     * 可以再设置一个group数组，数组的长度为max-min+1
     * 其中每个位置记录了一个值应该放在哪个桶中，这样当一个值进来，我们既可以直接通过
     * 数组索引的方式以常数时间复杂度来找到应该放入的桶的位置。
     * 但是这种方法也有一个缺点，就是当min和max之间的整数过多时(0,Integer.MAX_VALUE)
     * ，需要开辟的数组过大，导致爆栈。
     */
    /**
     * 下面考虑一种改进方法：
     * 因为传入的值都为整数，我们尽量均匀分布每个桶内存放整数的个数（这里指的是除了最后一个桶外，
     * 每个桶内存放的整数的数量都一样）。让每个桶的左右边界都是整数
     * e.g.
     *  min = 7  max = 34 buckets = 6
     *  每个桶中存放的整数的个数 numPreBuckets = Math.ceil((34-7+1)/6) = 5
     *  则桶编号对应的桶中存放的整数为：
     *      编号       整数
     *      0           7，8，9，10，11
     *      1           12，13，14，15，16
     *      2           17，18，19，20，21
     *      3           22，23，24，25，26
     *      4           27，28，29，30，31
     *      5           32，33，34
     *      给定一个value ：29
     *      (29-7)/5 = 4 所以他在编号为4的组
     *      编号为4的组的上界为   （4+1）*5+7 - 1
     *      编号为4的组的下届为    4 * 5 + 7
     */
    private int buckets;

    private int min;

    private int max;

    private int []counts;

    private int numPreBucket;

    private int tupCounts;


    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.counts = new int[buckets];
        this.numPreBucket = (int)Math.ceil(((double) (max-min+1))/buckets);
        this.tupCounts = 0;
    }

    private int getGroup(int v){
        return (v-min)/this.numPreBucket;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int group = this.getGroup(v);
        this.counts[group]++;
        this.tupCounts++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        if(zeroProb(op,v)){
            return 0.0;
        }
        if(oneProb(op,v)){
            return 1.0;
        }
        int group = this.getGroup(v);
        int left = group*this.numPreBucket + min;
        int right = (group == buckets-1) ? max: (group+1)*this.numPreBucket-1;
        if(this.numPreBucket == 1){
            right = left + 1;
        }
        if(Predicate.Op.NOT_EQUALS.equals(op)){
            return 1-(double)this.counts[group]/(right-left)/this.tupCounts;
        }
        if(Predicate.Op.EQUALS.equals(op)){
            return (double)this.counts[group]/(right-left)/this.tupCounts;
        }
        if(Predicate.Op.GREATER_THAN.equals(op)||Predicate.Op.GREATER_THAN_OR_EQ.equals(op)){
            double res = 0;
            int tups = 0;
            res +=(double)(right-v)/(right-left)*this.counts[group]/this.tupCounts;
            if (right == v){
                res += (double)(right-left)*this.counts[group]/this.tupCounts;
            }
            for(int i = group+1; i < this.buckets ; i++){
                tups += this.counts[i];
            }
            res += (double)tups/this.tupCounts;
            return res;
        }
        if(Predicate.Op.LESS_THAN.equals(op)||Predicate.Op.LESS_THAN_OR_EQ.equals(op)){
            double res = 0;
            int tups = 0;
            res += (double)(v-left)/(right-left)*this.counts[group]/this.tupCounts;
            if (v  == left){
                res += (double)(right-left)*this.counts[group]/this.tupCounts;
            }
            for(int i = group-1; i >=0 ; i--){
                tups += this.counts[i];
            }
            res += (double)tups/this.tupCounts;
            return res;
        }
        return -1.0;
    }

    private boolean zeroProb(Predicate.Op op, int v){
        if ((Predicate.Op.LESS_THAN.equals(op)||Predicate.Op.LESS_THAN_OR_EQ.equals(op))
                && v <= min){
            return true;
        }
        if((Predicate.Op.GREATER_THAN.equals(op)||Predicate.Op.GREATER_THAN_OR_EQ.equals(op))
                && v >= max){
            return true;
        }
        if(Predicate.Op.EQUALS.equals(op) && (v < min || v > max)){
            return true;
        }
        return false;
    }
    private boolean oneProb(Predicate.Op op, int v){
        if ((Predicate.Op.LESS_THAN.equals(op)||Predicate.Op.LESS_THAN_OR_EQ.equals(op))
                && v >= max){
            return true;
        }
        if((Predicate.Op.GREATER_THAN.equals(op)||Predicate.Op.GREATER_THAN_OR_EQ.equals(op))
                && v <= min){
            return true;
        }
        if(Predicate.Op.NOT_EQUALS.equals(op) && (v < min || v > max)){
            return true;
        }
        return false;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("Information of This IntHistogram:\n" +
                "min %d, max: %d buckets %d\n",min,max,buckets);
    }
}
