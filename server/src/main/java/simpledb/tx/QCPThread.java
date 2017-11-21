/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.tx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.CheckpointRecord;
import simpledb.tx.recovery.LogRecord;
import static simpledb.tx.recovery.LogRecord.CHECKPOINT;
import static simpledb.tx.recovery.LogRecord.COMMIT;
import static simpledb.tx.recovery.LogRecord.ROLLBACK;


/**
 *
 * @author gardhi01
 */
public class QCPThread {
    private static Integer chkptLock;
    public static boolean inprogress = false;
    private ArrayList<Transaction> currentTransaction = Transaction.runningT;
    private ArrayList<Integer> txnums = Transaction.txnumList;
    
    public QCPThread() {
        for (int i=0; i<10; i++) {
            Transaction t = new Transaction();
        }
        if (txnums.size() == 10) {
            run();
        }
        for (int i=0; i<5; i++) {
            Transaction t = new Transaction();
        }
    }
    
    
    // should stop new transactions from being started
    
    public void run() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {}
        while (currentTransaction.size() != 0) {
            synchronized (Transaction.lock) {
                try {
                inprogress = true;
                chkptLock.wait();
                } catch (InterruptedException ex) {}
            }
        }
        doCheckpoint();
        txnums.clear();
        chkptLock.notifyAll();
        inprogress = false;

            
    }
    public void doCheckpoint() {
      for (int txn : txnums) {
        SimpleDB.bufferMgr().flushAll(txn);
      }
      int lsn = new CheckpointRecord().writeToLog();
      SimpleDB.logMgr().flush(lsn);

   }
    
   
    
}
    

