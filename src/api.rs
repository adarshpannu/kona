#![allow(warnings)]
struct FlareContext {}
impl FlareContext {
    fn textFile(&self, name: String) -> TextFileRDD {
        TextFileRDD { name }
    }
}

trait RDD {
    type Item;

    fn next(&mut self) -> Option<Self::Item>;

    fn map<F, U>(self, mapfn: F) -> MapRDD<F, Self>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> U,
    {
        MapRDD {
            parent: self,
            mapfn,
        }
    }
}

struct TextFileRDD {
    name: String,
}

impl RDD for TextFileRDD {
    type Item = String;
    fn next(&mut self) -> Option<String> {
        None
    }
}

struct MapRDD<F, R> {
    parent: R,
    mapfn: F,
}

#[test]
fn test() {
    let fc = FlareContext {};
    let rdd = fc.textFile("./foo.txt".to_owned()).map(|e| e.len());
}
