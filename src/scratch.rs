
use crate::row::*;
use crate::includes::*;

#[test]
fn test_vec() {
    let mut v = vec![];

    for i in 0..10 {
        v.push(DataType::UNKNOWN)
    }

    replace(&mut v[1], DataType::BOOL);
    replace(&mut v[2], DataType::BOOL);

    let n = 10;
    let v = vec![Datum::NULL; n];
    dbg!(&v);

    let v2 = &v[..];
    dbg!(v2.len());
}
