package simpledb.buffer;

import simpledb.file.*;

import java.sql.Timestamp;
import java.time.Instant;


/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private int strategy;
   private int timeCounter = 0;
   private int clockhand = 0;

   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++)
         bufferpool[i] = new Buffer();
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         // set time in
         this.timeCounter ++;
         buff.setTimeIn(this.timeCounter);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      // set time out
      this.timeCounter ++;
      buff.setTimeOut(this.timeCounter);
      if (!buff.isPinned())
         numAvailable++;
   }
   
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
      for (Buffer buff : bufferpool) {
         Block b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;
   }
   // pass algorithm here Naive(0), FIFO(1), LRU(2), Clock(3)
   private Buffer chooseUnpinnedBuffer() {

   // isPinned looks at how many pins a buffer has

     switch (this.strategy) {
      case 0:
       return useNaiveStrategy();
      case 1:
        return useFIFOStrategy();
      case 2:
        return useLRUStrategy();
      case 3:
        return useClockStrategy();
      default:
        return null;
     }
   }
   /**
    * @return Allocated buffers
    */
   public Buffer[] getBuffers() {
     return this.bufferpool;
   }
   /**
    * Set buffer selection strategy
    * @param s (0 - Naive, 1 - FIFO, 2 - LRU, 3 - Clock)
    */
   public void setStrategy(int s) {
     this.strategy = s;
   }
   /**
    * Naive buffer selection strategy
    * @return 
    */
   private Buffer useNaiveStrategy() {
      int index = -1;
      for (Buffer buff : bufferpool) {
         index ++;
         if (!buff.isPinned()) {
            this.clockhand = index;
            return buff;
         }
      }
      return null;
   }
   /**
    * FIFO buffer selection strategy
    * @return 
    */
   // Instants and timestamps don't seem to be precise enough
   // to correctly implement FIFO and LRU
   // To correct this, I have added a timeCounter parameter to
   // this basicBufferManager class to keep an integer counter
   // that increments whenever a buffer is pinned or unpinned
   // The buffer class's getter and setter methods therefore 
   // get and set these integer values to be compared by 
   // the FIFO and LRU strategies
   private Buffer useFIFOStrategy() {
      Buffer chosen = null;
      int firstTimeIn = this.timeCounter;
      int index = -1;
      for (Buffer buff : bufferpool) {
          index ++;
          if (!buff.isPinned() && buff.getTimeIn() < firstTimeIn) {
              chosen = buff;
              firstTimeIn = buff.getTimeIn();
              this.clockhand = index;
          }
      }
      return chosen;
     // throw new UnsupportedOperationException();
   }
   /**
    * LRU buffer selection strategy
    * @return 
    */
   private Buffer useLRUStrategy() {
      Buffer chosen = null;
      int oldestTimeOut = this.timeCounter;
      int index = -1;
      for (Buffer buff : bufferpool) {
          index ++;
          if (!buff.isPinned() && buff.getTimeOut() < oldestTimeOut) {
              chosen = buff;
              oldestTimeOut = buff.getTimeOut();
              this.clockhand = index;
          }
      }
      return chosen;
   }
   /**
    * Clock buffer selection strategy
    * @return 
    */
   // I added a clockhand parameter to the basicBufferManager class
   // in order to keep track of which bufferpool index the clock should be pointing
   // to. This index is updated whenever a buffer is replaced,
   // and that can be seen in the other strategies too.
   private Buffer useClockStrategy() {
       int clockPosition = this.clockhand + 1;
       if (clockPosition > bufferpool.length - 1) {
           clockPosition = 0;
       }
       while (clockPosition != this.clockhand) {
           if (!bufferpool[clockPosition].isPinned()) {
               this.clockhand = clockPosition;
               return bufferpool[clockPosition];
           }
           clockPosition ++;
           if (clockPosition > bufferpool.length - 1) {
               clockPosition = 0;
           }
       }
       
       return null;
   }
}
