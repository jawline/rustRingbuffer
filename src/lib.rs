use std::{mem, alloc::{self, Layout}};
use std::sync::{atomic::{AtomicUsize, AtomicBool, Ordering}, Arc};

/**
 * A ring buffer is a queue data structure with a fixed capacity where items are written into a circular buffer.
 *
 * This ring buffer is implemented using direct memory allocations and pointers from the unsafe language subset.
 *
 * To use this implementation between two threads use the 'threadsafe' method which produces a Producer (a thing that can write to the Rb) and a Consumer (a thing that can read from the Rb). There can ever only be one producer and one consumer, it is unsafe (and not allowed by rusts borrow checker) to clone the producer or consumer without a lock.
 *
 */
pub struct Ringbuffer<T> {

  /// The data is stored at this address in a linear allocation
  data: *mut T,

  /// The layout of this buffer, needed for dealloc
  data_layout: Layout,

  /// The total capacity (measured in number of T, not bytes) of this ring buffer
  capacity: usize,

  /// The index of the next item to be written
  /// If the write buffer is full this will be set to capacity
  write: AtomicUsize,

  /// The index of the next item to be read
  read: AtomicUsize,
}

impl <T>Ringbuffer<T> {

  /// Create a fresh ringbuffer that can store up to capacity items in it's queue.
  pub fn new(capacity: usize) -> Self {

    // Unsafe rust malloc is a little nasty, we need to create a layout and then dealloc with the same alloc (any other layout to dealloc is UB)
    let memory_layout = Layout::from_size_align(capacity * mem::size_of::<T>(), mem::align_of::<T>()).expect("cannot created ring buffer memory layout");

    // Allocate memory for the RB, store the layout and initialize the read / write heads.
    unsafe {
      Self {
        data: alloc::alloc(memory_layout) as *mut T,
        data_layout: memory_layout,
        capacity: capacity,
        write: AtomicUsize::new(0),
        read: AtomicUsize::new(0),
      }
    }
  }

  pub fn threadsafe(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let new_rb = Arc::new(Ringbuffer::new(capacity));
    (Producer { rb: new_rb.clone() }, Consumer { rb: new_rb.clone() })
  }

  fn next(&self, index: usize) -> usize {
    (index + 1) % self.capacity
  }

  unsafe fn item(&self, index: usize) -> *mut T {
    self.data.offset(index as isize)
  }

  fn current(&self) -> (usize, usize) {
    (self.read.load(Ordering::Acquire), self.write.load(Ordering::Acquire))
  }

  pub fn take(&mut self) -> Option<T> {
    let (read, write) = self.current();
    if read == write {
      None
    } else {

      let rval;

      unsafe {
        rval = self.item(read).read();
      }

      self.read.store(self.next(read), Ordering::Release);

      // Now that the read head has moved, unlock write it if was stuck due to capacity
      if write == self.capacity {
        self.write.store(read, Ordering::Release);
      }

      Some(rval)
    }
  }

  pub fn add(&mut self, v: T) -> Result<(), ()> {
    let (read, write) = self.current();

    if write == self.capacity {
      // No space available
      Err(())
    } else {

      unsafe {
        self.item(write).write(v);
      }

      let write = self.next(write);

      // TODO: Since we decide that we are full based on potentially stale data
      // there is a change that read has already changed here. This cannot lead
      // to corruption but could lead to write stalling.
      // Should fix this later.

      if write == read {
        self.write.store(self.capacity, Ordering::Release);
      } else {
        self.write.store(write, Ordering::Release);
      }

      Ok(())
    }
  }

  pub fn len(&self) -> usize {
    let (read, write) = self.current();
    if write == self.capacity {
      self.capacity
    } else if write < read {
      write + (self.capacity - read)
    } else {
      write - read
    }
  }
}

impl <T>Drop for Ringbuffer<T> {
  fn drop(&mut self) {
    unsafe {
      alloc::dealloc(self.data as *mut u8, self.data_layout);
    }
  }
}

pub struct Producer<T> {
  rb: Arc<Ringbuffer<T>>
}

impl <T>Producer<T> {
  pub fn add(&self, item: T) -> Result<(), ()> {
    unsafe {
      let rb_ptr = Arc::as_ptr(&self.rb) as *mut Ringbuffer<T>;
      let rb_mut: &mut Ringbuffer<T> = rb_ptr.as_mut().unwrap();
      rb_mut.add(item)
    }
  }
}

pub struct Consumer<T> {
  rb: Arc<Ringbuffer<T>>
}

impl <T>Consumer<T> {
  pub fn take(&self) -> Option<T> {
    unsafe {
      let rb_ptr = Arc::as_ptr(&self.rb) as *mut Ringbuffer<T>;
      let rb_mut: &mut Ringbuffer<T> = rb_ptr.as_mut().unwrap();
      rb_mut.take()
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn basic_ring() {
    let mut ring = Ringbuffer::new(50);
    ring.add(5).unwrap();
    ring.add(6).unwrap();
    ring.add(7).unwrap();

    assert_eq!(ring.len(), 3);

    assert_eq!(ring.take().unwrap(), 5);
    assert_eq!(ring.take().unwrap(), 6);
    assert_eq!(ring.take().unwrap(), 7);

    assert_eq!(ring.len(), 0);
  }

  #[test]
  fn empty_ring() {
    let mut ring = Ringbuffer::<u32>::new(50);
    assert_eq!(ring.len(), 0);
    assert_eq!(ring.take(), None);
  }

  #[test]
  fn filled_ring_stops() {
    let mut ring = Ringbuffer::new(3);
    ring.add(5).unwrap();
    ring.add(6).unwrap();
    ring.add(7).unwrap();
    assert!(ring.add(8).is_err());
    assert!(ring.add(9).is_err());

    assert_eq!(ring.len(), 3);

    assert_eq!(ring.take().unwrap(), 5);
    assert_eq!(ring.take().unwrap(), 6);
    assert_eq!(ring.take().unwrap(), 7);

    assert_eq!(ring.len(), 0);

    assert_eq!(ring.take(), None);
  }

  #[test]
  fn roll_over_test() {
    let mut ring = Ringbuffer::new(100);

    for _ in 0..10000 {
      for i in 0..100 {
        ring.add(i).unwrap();
      }
      for i in 0..100 {
        assert_eq!(ring.take().unwrap(), i);
      }
    }

    println!("FIN");
  }

  #[test]
  fn threasafe_test() {
    let (tx, rx) = Ringbuffer::threadsafe(100);

    for _ in 0..10000 {
      for i in 0..100 {
        tx.add(i).unwrap();
      }
      for i in 0..100 {
        assert_eq!(rx.take().unwrap(), i);
      }
    }

    println!("FIN");
  }
}
