
use crate::row::*;
use crate::includes::*;

use bitmaps::Bitmap;

#[test]
fn test_vec() {
    let mut bitmap: Bitmap<10> = Bitmap::new();
    bitmap.set(8, true);

    let mut bitmap2: Bitmap<10> = Bitmap::new();
    bitmap2.set(3, true);

    let x = bitmap | bitmap2;

    for b in x.into_iter() {
        dbg!(&b);
    }

    dbg!(std::mem::size_of::<Bitmap<256>>());

}
