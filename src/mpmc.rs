use std::{cmp::min, mem, alloc::{self, Layout}};
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};

pub struct ThreadPos {
  pub write: AtomicUsize,
  pub read: AtomicUsize,
}

pub struct Ringbuffer<T: Send> {

  /// The data is stored at this address in a linear allocation
  data: *mut T,

  /// The current thread positions
  thread_data: Vec<ThreadPos>,

  /// The number of threads
  num_threads: usize,

  /// The layout of this buffer, needed for dealloc
  data_layout: Layout,

  /// The total capacity (measured in number of T, not bytes) of this ring buffer
  capacity: usize,

  /// The mask (pow2 - 1) to index into the array (much faster than mod)
  item_mask: usize,

  /// The index of the next item to be written
  write: AtomicUsize,

  /// The index of the highest item written (since there can be multiple producers they reserve a slot and then write it)
  written: AtomicUsize,

  /// The index of the next item to be read
  reading: AtomicUsize,

  /// The index of the highest fully read item in the ring (multiple consumers so one reserves an item to read by incrementing reading then reads it by incrementing read)
  read: AtomicUsize,
}

unsafe impl <T: Send> Send for Ringbuffer<T> {}
unsafe impl <T: Send> Sync for Ringbuffer<T> {}

impl <T: Send>Ringbuffer<T> {

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
  pub fn new(capacity: usize, threads: usize) -> Result<Self, ()> {

    // Check the capacity is a power of two
    if !Ringbuffer::<T>::check_power_of_two(capacity) {
      return Err(());
    }

    // Unsafe rust malloc is a little nasty, we need to create a layout and then dealloc with the same alloc (any other layout to dealloc is UB)
    let memory_layout = Layout::from_size_align(capacity * mem::size_of::<T>(), mem::align_of::<T>()).expect("cannot created ring buffer memory layout");

    let mut thread_data = Vec::new();

    for _ in 0..threads {
      thread_data.push(ThreadPos { write: AtomicUsize::new(usize::MAX), read: AtomicUsize::new(usize::MAX) });
    }

    // Allocate memory for the RB, store the layout and initialize the read / write heads.
    Ok(Self {
      data: unsafe { alloc::alloc(memory_layout) as *mut T },
      data_layout: memory_layout,
      thread_data: thread_data,
      num_threads: threads,
      capacity: capacity,
      item_mask: capacity - 1, /* this works only because capacity is a power of two so 100 becomes 011 (any bit will be a valid index) */
      write: AtomicUsize::new(0),
      read: AtomicUsize::new(0),
      written: AtomicUsize::new(0),
      reading: AtomicUsize::new(0),
    })
  }

  pub fn threadsafe(capacity: usize, threads: usize) -> Result<(Vec<Producer<T>>, Vec<Consumer<T>>), ()> {
    let new_rb = Arc::new(Ringbuffer::new(capacity, threads * 2)?);

    let mut producers = Vec::new();
    let mut consumers = Vec::new();

    for i in 0..threads {
      producers.push(Producer{ id: i, rb: new_rb.clone() });
      consumers.push(Consumer{ id: i + threads, rb: new_rb.clone() });
    }

    Ok((producers, consumers))
  }

  unsafe fn item(&self, index: usize) -> *mut T {
    self.data.offset((index & self.item_mask) as isize)
  }

  fn current(&self) -> (usize, usize) {
    (self.read.load(Ordering::Acquire), self.write.load(Ordering::Acquire))
  }

  pub fn take(&mut self, thread: usize) -> T {

    let read_pos = self.reading.fetch_add(1, Ordering::Release);
    self.thread_data[thread].read.store(read_pos, Ordering::Relaxed);

    let num_threads = self.num_threads;

    while read_pos >= self.written.load(Ordering::Acquire) {
      let mut min_write = self.write.load(Ordering::Relaxed);

      for thread in 0..num_threads {
        min_write = min(self.thread_data[thread].write.load(Ordering::Relaxed), min_write);
      }

      self.written.store(min_write, Ordering::Relaxed);

      if read_pos >= min_write {
        break;
      }
    }

    let rval;

    unsafe {
      rval = self.item(read_pos).read();
    }

    self.thread_data[thread].read.store(usize::MAX, Ordering::Relaxed);

    rval
  }

  pub fn add(&mut self, v: T, thread: usize) {

    let write_pos = self.write.fetch_add(1, Ordering::Release);
    self.thread_data[thread].write.store(write_pos, Ordering::Relaxed);

    let capacity = self.capacity;
    let num_threads = self.num_threads;

    // Wait until there is space for this in the ring
    while self.read.load(Ordering::Acquire) + capacity < write_pos {
      let mut min_read = self.reading.load(Ordering::Relaxed);

      for thread in 0..num_threads {
        min_read = min(self.thread_data[thread].read.load(Ordering::Relaxed), min_read);
      }

      self.read.store(min_read, Ordering::Relaxed);

      if min_read + capacity < write_pos {
        break;
      }

      // Maybe do a micro sleep here?
    }

    // We now know that this space has been reserved just for us now
    unsafe {
      self.item(write_pos).write(v);
    }

    self.thread_data[thread].write.store(usize::MAX, Ordering::Relaxed);
  }

  pub fn len(&self) -> usize {
    let (read, write) = self.current();
    write - read
  }
}

impl <T: Send> Drop for Ringbuffer<T> {
  fn drop(&mut self) {
    unsafe {
      alloc::dealloc(self.data as *mut u8, self.data_layout);
    }
  }
}

pub struct Producer<T: Send> {
  rb: Arc<Ringbuffer<T>>,
  id: usize,
}

impl <T: Send>Producer<T> {
  pub fn add(&self, item: T) {
    unsafe {
      let rb_ptr = Arc::as_ptr(&self.rb) as *mut Ringbuffer<T>;
      let rb_mut: &mut Ringbuffer<T> = rb_ptr.as_mut().unwrap();
      rb_mut.add(item, self.id)
    }
  }
}

pub struct Consumer<T: Send> {
  rb: Arc<Ringbuffer<T>>,
  id: usize,
}

impl <T: Send>Consumer<T> {
  pub fn take(&self) -> T {
    unsafe {
      let rb_ptr = Arc::as_ptr(&self.rb) as *mut Ringbuffer<T>;
      let rb_mut: &mut Ringbuffer<T> = rb_ptr.as_mut().unwrap();
      rb_mut.take(self.id)
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::thread::spawn;

  #[test]
  fn threasafe_test() {
    let (mut tx, mut rx) = Ringbuffer::threadsafe(128, 1).unwrap();
    let (tx, rx) = (tx.pop().unwrap(), rx.pop().unwrap());

    spawn(move || {
      for _ in 0..100000 {
        for i in 0..100 {
          tx.add(i);
        }
      }
    });

    spawn(move || {
      for _ in 0..100000 {
        for i in 0..100 {
          assert_eq!(rx.take(), i);
        }
      }
    });

    println!("FIN");
  }
}
