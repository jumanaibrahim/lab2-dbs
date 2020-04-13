package simpledb;

import java.awt.image.ImageProducer;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Object, ArrayList<Integer>> aggregates; //aggregator without grouping
//    private HashMap<Object, Integer> retMap;
//    private ArrayList<Integer> valueList;
//    private ArrayList<String> keyList;


    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield; //index of GB field
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregates = new HashMap<Object, ArrayList<Integer>>();
//        this.retMap = new HashMap<Field, Integer>();
//        this.keyList = new ArrayList<String>();
//        this.valueList = new ArrayList<Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field tupGBField = null;

        if(gbfield==Aggregator.NO_GROUPING){
            if(!aggregates.containsKey("")){
                aggregates.put("",new ArrayList<>());
            }
            aggregates.get("").add(((IntField) tup.getField(afield)).getValue()); //ASK MIRO IF THIS IS WHERE YOU ADD STUFF
        } else {tupGBField = tup.getField(gbfield);} //get the field from the index

        if(gbfieldtype == Type.INT_TYPE){
            assert tupGBField != null;
            Integer groupKey = ((IntField) tupGBField).getValue();
            if(!aggregates.containsKey(groupKey)){
                aggregates.put(groupKey, new ArrayList<>());
            }
            Integer value = ((IntField) tup.getField(afield)).getValue();
            aggregates.get(groupKey).add(value);

        } else if (gbfieldtype == Type.STRING_TYPE){
            assert tupGBField != null;
            String groupKey = ((StringField) tupGBField).getValue();
            if(!aggregates.containsKey(groupKey)){
                aggregates.put(groupKey, new ArrayList<>());
            }
            Integer value = ((IntField) tup.getField(afield)).getValue();
            aggregates.get(groupKey).add(value);
        }


//        int value = ((IntField) tup.getField(afield)).getValue(); //get value of integer in aggregate field
//
//        if(!aggregates.containsKey(tupGBField)){
//            ArrayList<Integer> values = new ArrayList<Integer>();
//            values.add(value); //add value to values list
//            aggregates.put(tupGBField,values);ArrayList<Integer> data = i.getValue();
//        } else {
//            aggregates.get(tupGBField).add(value);
//        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        AggregateIter aggregateIter = new AggregateIter();
        return aggregateIter;
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
    }

    private class AggregateIter implements OpIterator{
        private ArrayList<Tuple> ret = new ArrayList<Tuple>();
        private Iterator<Tuple> it;

        public AggregateIter() {
            for (Map.Entry<Object, ArrayList<Integer>> i : aggregates.entrySet()) {
                Tuple entryTup = new Tuple(this.getTupleDesc());
                int val = 0;
                switch (what) {
                    case MIN:
                        val = Collections.min(i.getValue());
                        break;
                    case MAX:
                        val = Collections.max(i.getValue());
                        break;
                    case SUM:
                        val = i.getValue().stream().mapToInt(Integer::intValue).sum();
                        break;
                    case AVG:
                        int size = i.getValue().size();
                        int sum = i.getValue().stream().mapToInt(Integer::intValue).sum();
                        val = sum / size;
                        break;
                    case COUNT:
                        val = i.getValue().size();
                        break;
                    case SUM_COUNT:
                        break;
                    case SC_AVG:
                        break;
                }
                if(gbfieldtype == null){
                    entryTup.setField(0, new IntField(val));
                }
                else{
                    if (gbfieldtype == Type.INT_TYPE) {
                        entryTup.setField(0, new IntField((Integer) i.getKey()));
                    } else {
                        entryTup.setField(0, new StringField((String) i.getKey(), i.getKey().toString().length()));
                    }
                    entryTup.setField(1, new IntField(val));
                }
                ret.add(entryTup);
            }
        }
//            switch (what){
//                case MIN:
//                    for(Map.Entry<Object, ArrayList<Integer>> i : aggregates.entrySet()){
//                        Tuple entryTup = new Tuple(this.getTupleDesc());
//                        int min = Collections.min(i.getValue());
//                        if(!(gbfieldtype==null)){
//                            if(gbfieldtype==Type.INT_TYPE){
//                                entryTup.setField(0, new IntField((Integer) i.getKey()));
//                            } else{entryTup.setField(0, new StringField((String) i.getKey() , i.getKey().toString().length()));}
////                            entryTup.setField(0, (Field) i.getKey());
//                            entryTup.setField(1, new IntField(min));
//                        } else{entryTup.setField(1,new IntField(min));}
//                        ret.add(entryTup);
//                    }
//                    break;
//                case MAX:
//                    for(Map.Entry<Object, ArrayList<Integer>> i : aggregates.entrySet()){
//                        Tuple entryTup = new Tuple(this.getTupleDesc());
//                        int max = Collections.max(i.getValue());
//                        if(!(gbfieldtype==null)){
//                            if(gbfieldtype==Type.INT_TYPE){
//                                entryTup.setField(0, new IntField((Integer) i.getKey()));
//                            } else{entryTup.setField(0, new StringField((String) i.getKey() , i.getKey().toString().length()));}
////                            entryTup.setField(0, (Field) i.getKey());
//                            entryTup.setField(1, new IntField(max));
//                        } else{entryTup.setField(1,new IntField(max));}
//                        ret.add(entryTup);
//                    }
//                    break;
//                case SUM:
//                    for(Map.Entry<Object, ArrayList<Integer>> i : aggregates.entrySet()){
//                        Tuple entryTup = new Tuple(this.getTupleDesc());
//                        int sum = i.getValue().stream().mapToInt(Integer::intValue).sum();
//                        if(!(gbfieldtype==null)){
//                            if(gbfieldtype==Type.INT_TYPE){
//                                entryTup.setField(0, new IntField((Integer) i.getKey()));
//                            } else{entryTup.setField(0, new StringField((String) i.getKey() , i.getKey().toString().length()));}
////                            entryTup.setField(0, (Field) i.getKey());
//                            entryTup.setField(1, new IntField(sum));
//                        } else{entryTup.setField(1,new IntField(sum));}
//                        ret.add(entryTup);
//                    }
//                    break;
//                case AVG:
//                    for(Map.Entry<Object,ArrayList<Integer>> i : aggregates.entrySet()){
//                        Tuple entryTup = new Tuple(this.getTupleDesc());
//                        int size = i.getValue().size();
//                        int sum = i.getValue().stream().mapToInt(Integer::intValue).sum();
//                        int avg = sum/size;
//                        if(!(gbfieldtype==null)){
//                            if(gbfieldtype==Type.INT_TYPE){
//                                entryTup.setField(0,new IntField((Integer) i.getKey()));
//                            } else{entryTup.setField(0, new StringField((String) i.getKey() , i.getKey().toString().length()));}
//                            entryTup.setField(1,new IntField(avg));
//                        } else {entryTup.setField(1,new IntField(avg));}
//                        ret.add(entryTup);
//                    }
//                    break;
//                case COUNT:
//
//                    break;
//                case SUM_COUNT:
//                    break;
//                case SC_AVG:
//                    break;
//            }
//        }
        //make distinction between grouped/ungrouped: based on key being empty string.
        //for each value in map, create tuple based on the 'what'


        @Override
        public void open() throws DbException, TransactionAbortedException {
            //initialize it to beginning of ret;
            it = ret.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // it.next
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            //same as open
            it = ret.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            //either non grouped aggregator -- so one column with aggregate value
            //or two columns: value grouped by, integer

            if(gbfield==Aggregator.NO_GROUPING){
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            } else{
                return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            }
        }

        @Override
        public void close() {
            it = null;
        }
    }
}
