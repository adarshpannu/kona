#![allow(warnings)]
struct FlareContext;

impl FlareContext {
    fn textFile(&self, name: String) -> TextFileRDD {
        TextFileRDD { name }
    }
}

trait RDDBase {
    type T;

    fn map<F, U>(self, mapfn: F) -> MapRDD<F, Self>
    where
        Self: Sized,
        F: FnMut(Self::T) -> U,
    {
        MapRDD {
            mapfn,
            source: self,
        }
    }
}

struct MapRDD<F, T> {
    mapfn: F,
    source: T,
}

struct TextFileRDD {
    name: String,
}

impl RDDBase for TextFileRDD {
    type T = String;
}

#[test]
fn test() {
    let fc = FlareContext {};

    let tf = fc.textFile("./foo.csv".to_owned()).map(|e| e.len());
}
