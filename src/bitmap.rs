use std::hash::Hash;
use std::collections::HashMap;

enum Bitmap {
    Static(usize),
    Dynamic(usize)
}

struct DynBitmap<T>
where
    T: Hash,
{
    v: Vec<u8>,
    dict: HashMap<T, usize>
}

impl<T> DynBitmap<T>
where
    T: Hash,
{
    fn new(initial_element_count: usize) -> DynBitmap<T> {
        let vlen = (initial_element_count / 8).max(16);
        let v = vec![0; initial_element_count / 8];
        let dict = HashMap::new();
        DynBitmap { v, dict }
    }

    fn set(elem: T) {
        
    }
}

#[test]
fn test() {}
