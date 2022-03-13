// Use bitmaps on arbitrary bitmaps

use bitmaps::Bitmap;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

#[derive(Debug)]
pub struct Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    pub bitmap: Bitmap<256>,
    dict: Rc<RefCell<HashMap<T, usize>>>,
    rev_dict: Rc<RefCell<HashMap<usize, T>>>,
    next_id: Rc<RefCell<usize>>,
}

impl<T> Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    pub fn new() -> Bitset<T> {
        Bitset {
            bitmap: Bitmap::new(),
            dict: Rc::new(RefCell::new(HashMap::new())),
            rev_dict: Rc::new(RefCell::new(HashMap::new())),
            next_id: Rc::new(RefCell::new(0)),
        }
    }

    pub fn set(&mut self, elem: T) {
        // Check if elem exists in HashMap. If not, add it.
        let mut dict = (*self.dict).borrow_mut();
        let mut rev_dict = (*self.rev_dict).borrow_mut();

        let ix = dict.entry(elem).or_insert_with(|| {
            let mut id = (*self.next_id).borrow_mut();
            let retval = *id;
            *id = *id + 1;
            retval
        });
        self.bitmap.set(*ix, true);
        rev_dict.insert(*ix, elem);
    }

    pub fn get(&self, elem: T) -> bool {
        let ix = *self.dict.borrow().get(&elem).unwrap();
        self.bitmap.get(ix)
    }

    pub fn set_direct(&mut self, ix: usize) {
        self.bitmap.set(ix, true);
    }

    pub fn get_direct(&self, ix: usize) -> bool {
        self.bitmap.get(ix)
    }

    pub fn clear(mut self) -> Self {
        self.bitmap = Bitmap::new();
        self
    }

    pub fn elements(&self) -> Vec<T> {
        let bitmap = self.bitmap.clone();
        bitmap.into_iter().map(|ix| {
            let rev_dict = (*self.rev_dict).borrow();
            *rev_dict.get(&ix).unwrap()
        }).collect()
    }
}

impl<T> Clone for Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    fn clone(&self) -> Bitset<T> {
        Bitset {
            bitmap: self.bitmap.clone(),
            dict: Rc::clone(&self.dict),
            rev_dict: Rc::clone(&self.rev_dict),
            next_id: Rc::clone(&self.next_id),
        }
    }
}
