// bitset: Thin wrapper over bitmaps::Bitmap so they work over datatypes

use std::{
    cell::RefCell,
    collections::HashMap,
    hash::Hash,
    ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign},
    rc::Rc,
};

use bitmaps::Bitmap;

#[derive(Debug, PartialEq, Eq)]
pub struct Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy, {
    bitmap: Bitmap<256>,
    dict: Rc<RefCell<HashMap<T, usize>>>,
    rev_dict: Rc<RefCell<HashMap<usize, T>>>,
    next_id: Rc<RefCell<usize>>,
}

impl<T> Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    pub fn are_clones(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.dict, &other.dict) && Rc::ptr_eq(&self.rev_dict, &other.rev_dict)
    }

    pub fn set(&mut self, elem: T) {
        // Check if elem exists in HashMap. If not, add it.
        let mut dict = (*self.dict).borrow_mut();
        let mut rev_dict = (*self.rev_dict).borrow_mut();

        let ix = dict.entry(elem).or_insert_with(|| {
            let mut id = (*self.next_id).borrow_mut();
            let retval = *id;
            *id += 1;
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

    pub fn clone_metadata(&self) -> Self {
        self.clone().clear()
    }

    pub fn elements(&self) -> Vec<T> {
        let bitmap = self.bitmap;
        let rev_dict = (*self.rev_dict).borrow();
        bitmap.into_iter().map(|ix| *rev_dict.get(&ix).unwrap()).collect()
    }

    pub fn len(&self) -> usize {
        self.bitmap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bitmap.len() == 0
    }

    pub fn init(mut self, it: impl Iterator<Item = T>) -> Self {
        for t in it {
            self.set(t)
        }
        self
    }

    pub fn is_subset_of(&self, other: &Self) -> bool {
        (self.bitmap & other.bitmap) == self.bitmap
    }

    pub fn is_disjoint(&self, other: &Self) -> bool {
        (self.bitmap & other.bitmap).is_empty()
    }
}

impl<T> Default for Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    fn default() -> Self {
        Bitset {
            bitmap: Bitmap::new(),
            dict: Rc::new(RefCell::new(HashMap::new())),
            rev_dict: Rc::new(RefCell::new(HashMap::new())),
            next_id: Rc::new(RefCell::new(0)),
        }
    }
}

impl<T> Clone for Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    fn clone(&self) -> Bitset<T> {
        Bitset {
            bitmap: self.bitmap,
            dict: Rc::clone(&self.dict),
            rev_dict: Rc::clone(&self.rev_dict),
            next_id: Rc::clone(&self.next_id),
        }
    }
}

impl<'a, T> BitAnd<&'a Bitset<T>> for Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    type Output = Self;

    // rhs is the "right-hand side" of the expression `a & b`
    fn bitand(self, rhs: &'a Bitset<T>) -> Self::Output {
        let mut other = self;
        other.bitmap &= rhs.bitmap;
        other
    }
}

impl<'a, T> BitAnd<&'a Bitset<T>> for &'a Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    type Output = Bitset<T>;

    // rhs is the "right-hand side" of the expression `a & b`
    fn bitand(self, rhs: &'a Bitset<T>) -> Self::Output {
        let mut other = self.clone();
        other.bitmap &= rhs.bitmap;
        other
    }
}

impl<T> BitAndAssign<Bitset<T>> for Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    fn bitand_assign(&mut self, rhs: Bitset<T>) {
        self.bitmap &= rhs.bitmap;
    }
}

impl<'a, T> BitOr<&'a Bitset<T>> for Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    type Output = Self;

    // rhs is the "right-hand side" of the expression `a & b`
    fn bitor(self, rhs: &'a Bitset<T>) -> Self::Output {
        let mut other = self;
        other.bitmap |= rhs.bitmap;
        other
    }
}

impl<'a, T> BitOr<&'a Bitset<T>> for &'a Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    type Output = Bitset<T>;

    // rhs is the "right-hand side" of the expression `a & b`
    fn bitor(self, rhs: &'a Bitset<T>) -> Self::Output {
        let mut other = self.clone();
        other.bitmap |= rhs.bitmap;
        other
    }
}

impl<'a, T> BitOrAssign<&'a Bitset<T>> for Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    fn bitor_assign(&mut self, rhs: &'a Bitset<T>) {
        self.bitmap |= rhs.bitmap;
    }
}

impl<T> AsRef<Bitset<T>> for Bitset<T>
where
    T: Hash + PartialEq + Eq + Copy,
{
    fn as_ref(&self) -> &Self {
        self
    }
}
