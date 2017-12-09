package simpledb.query;

import java.util.*;

/**
 * The scan class corresponding to the <i>project</i> relational
 * algebra operator.
 * All methods except hasField delegate their work to the
 * underlying scan.
 * @author Edward Sciore
 */
public class SemiJoinScan implements Scan {
   private Scan join;
   private Predicate pred;
   private Collection<String> fieldlist;
   
   /**
    * Creates a project scan having the specified
    * underlying scan and field list.
    * @param s1 
    * @param s2
    * @param pred 
    */
   public SemiJoinScan(Scan s1, Scan s2, Predicate pred, Collection<String> fieldlist) {
      this.join = new JoinScan(s1, s2, pred);
      this.pred = pred;
      this.fieldlist = fieldlist;
      join.next();
   }
   
   public void beforeFirst() {
      join.beforeFirst();
   }
   
   public boolean next() {
      while (join.next()) { 
          if (pred.isSatisfied(join)) {
              return true;
          }
      }
      return false;
   }
   
   public void close() {
      join.close();
   }
   
   public Constant getVal(String fldname) {
       if (hasField(fldname))
         return join.getVal(fldname);
      else
         throw new RuntimeException("field " + fldname + " not found.");
   }
   
   public int getInt(String fldname) {
       if (hasField(fldname))
         return join.getInt(fldname);
      else
         throw new RuntimeException("field " + fldname + " not found.");
   }
   
   public String getString(String fldname) {
       if (hasField(fldname))
         return join.getString(fldname);
      else
         throw new RuntimeException("field " + fldname + " not found.");
   }
   
   /**
    * Returns true if the specified field
    * is in the projection list.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
       return fieldlist.contains(fldname);
   }
}
