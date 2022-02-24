
use crate::row::*;

#[test]
fn test_vec() {
    let mut v = vec![];

    for i in 0..10 {
        v.push(DataType::UNKNOWN)
    }

    std::mem::replace(&mut v[1], DataType::BOOL);
    std::mem::replace(&mut v[2], DataType::BOOL);

    let n = 10;
    let v = vec![Datum::NULL; n];
    dbg!(&v);


}
