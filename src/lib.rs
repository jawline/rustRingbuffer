use std::{mem, alloc::{self, Layout}};
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};

/**
 * A ring buffer is a queue data structure with a fixed capacity where items are written into a circular buffer.
 * 
 * The capacity of the ring buffer must be a power of two to allow the speed up provided by using & (capacity - 1)
 * instead of % capacity. This works because when capacity is a power of two because capacity - 1 will return a bitmask that includes every possible value up to capacity and no values above it.
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

  /// The mask (pow2 - 1) to index into the array (much faster than mod)
  item_mask: usize,

  /// The index of the next item to be written
  write: AtomicUsize,

  /// The index of the next item to be read
  read: AtomicUsize,
}

impl <T>Ringbuffer<T> {

  fn check_power_of_two(mut capacity: usize) -> bool {
    let mut bits = 0;
    while capacity != 0 && bits <= 1 {
      if capacity & 0x1 == 0x1 {
        bits += 1;
      }
      capacity = capacity >> 1;
    }
    bits == 1
  }

  /// Create a fresh ringbuffer that can store up to capacity items in it's queue.
  /// The capacity must be a power of two.
  pub fn new(capacity: usize) -> Result<Self, ()> {

    // Check the capacity is a power of two
    if !Ringbuffer::<T>::check_power_of_two(capacity) {
      return Err(());
    }

    // Unsafe rust malloc is a little nasty, we need to create a layout and then dealloc with the same alloc (any other layout to dealloc is UB)
    let memory_layout = Layout::from_size_align(capacity * mem::size_of::<T>(), mem::align_of::<T>()).expect("cannot created ring buffer memory layout");

    // Allocate memory for the RB, store the layout and initialize the read / write heads.
    Ok(Self {
      data: unsafe { alloc::alloc(memory_layout) as *mut T },
      data_layout: memory_layout,
      capacity: capacity,
      item_mask: capacity - 1, /* this works only because capacity is a power of two so 100 becomes 011 (any bit will be a valid index) */
      write: AtomicUsize::new(0),
      read: AtomicUsize::new(0),
    })
  }

  pub fn threadsafe(capacity: usize) -> Result<(Producer<T>, Consumer<T>), ()> {
    let new_rb = Arc::new(Ringbuffer::new(capacity)?);
    Ok((Producer { rb: new_rb.clone() }, Consumer { rb: new_rb.clone() }))
  }

  unsafe fn item(&self, index: usize) -> *mut T {
    self.data.offset((index & self.item_mask) as isize)
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

      self.read.store(read + 1, Ordering::Release);

      Some(rval)
    }
  }

  pub fn add(&mut self, v: T) -> Result<(), ()> {
    let (read, write) = self.current();

    if write == read + self.capacity {
      // No space available
      Err(())
    } else {

      unsafe {
        self.item(write).write(v);
      }

      self.write.store(write + 1, Ordering::Release);

      Ok(())
    }
  }

  pub fn len(&self) -> usize {
    let (read, write) = self.current();
    write - read
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
    let mut ring = Ringbuffer::new(64).unwrap();
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
    let mut ring = Ringbuffer::<u32>::new(64).unwrap();
    assert_eq!(ring.len(), 0);
    assert_eq!(ring.take(), None);
  }

  #[test]
  fn non_power_of_two() {
    Ringbuffer::<u32>::new(8).unwrap();
    Ringbuffer::<u32>::new(16).unwrap();
    Ringbuffer::<u32>::new(65536).unwrap();
    assert!(Ringbuffer::<u32>::new(17).is_err());
    assert!(Ringbuffer::<u32>::new(19).is_err());
    assert!(Ringbuffer::<u32>::new(5213).is_err());
    assert!(Ringbuffer::<u32>::new(7).is_err());
  }

  #[test]
  fn filled_ring_stops() {
    let mut ring = Ringbuffer::new(4).unwrap();
    ring.add(5).unwrap();
    ring.add(6).unwrap();
    ring.add(7).unwrap();
    ring.add(8).unwrap();
    assert!(ring.add(8).is_err());
    assert!(ring.add(9).is_err());

    assert_eq!(ring.len(), 4);

    assert_eq!(ring.take().unwrap(), 5);
    assert_eq!(ring.take().unwrap(), 6);
    assert_eq!(ring.take().unwrap(), 7);
    assert_eq!(ring.take().unwrap(), 8);

    assert_eq!(ring.len(), 0);

    assert_eq!(ring.take(), None);
  }

  #[test]
  fn roll_over_test() {
    let mut ring = Ringbuffer::new(128).unwrap();

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
    let (tx, rx) = Ringbuffer::threadsafe(128).unwrap();

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
