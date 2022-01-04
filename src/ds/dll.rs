/// double linked list
use std::ptr;

pub struct Node<Item> {
    pub item: Item,
    pub prev: *mut Node<Item>,
    pub next: *mut Node<Item>,
}

impl<Item> Node<Item> {
    pub fn new(item: Item) -> Self {
        Self {
            item,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }

    pub fn remove(&mut self) {
        if !self.prev.is_null() {
            unsafe {
                (*self.prev).next = self.next;
            }
        }

        if !self.next.is_null() {
            unsafe {
                (*self.next).prev = self.prev;
            }
        }

        self.next = ptr::null_mut();
        self.prev = ptr::null_mut();
    }
}

pub struct DoublyLinkedList<Item> {
    pub head: *mut Node<Item>,
    pub tail: *mut Node<Item>,
    pub size: usize,
}

impl<Item> Default for DoublyLinkedList<Item> {
    fn default() -> Self {
        let head = ptr::null_mut();
        let tail = ptr::null_mut();

        Self {
            head,
            tail,
            size: 0,
        }
    }
}

impl<Item> DoublyLinkedList<Item> {
    pub fn push_head(&mut self, item: Item) -> *mut Node<Item> {
        self.size += 1;
        let node = Node::new(item);

        if self.head.is_null() {
            self.head = Box::into_raw(Box::new(node));
        }

        todo!()
    }

    pub fn push_tail(&mut self, item: Item) -> *mut Node<Item> {
        self.size += 1;
        let node = Node::new(item);

        todo!()
    }

    pub fn len(&self) -> usize {
        self.size
    }
}

impl<Item> Drop for DoublyLinkedList<Item> {
    fn drop(&mut self) {
        todo!()
    }
}
