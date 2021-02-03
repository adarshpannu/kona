
#![allow(warnings)]
struct FlareContext;

impl FlareContext {
    fn textFile(&self, name: String) -> TextFileRDD {
        unimplemented!()
    }
}

trait RDDBase<T> {

}

struct TextFileRDD {

}

impl RDDBase<String> for TextFileRDD {

}

#[test]
fn test() {
    let fc = FlareContext;
    let tf = fc.textFile("./foo.csv".to_owned());

}
