#![allow(warnings)]
struct FlareContext {

}

trait RDD<T> {
    fn next() -> Option<T>;
    //fn map<U>(mapfn: Fn(T) -> U);
    //fn filter(filterfn: Fn(T) -> bool);
}

impl FlareContext {
    fn textFile(&self, name: String) -> TextFileRDD {
        TextFileRDD { name }
    }
}

struct TextFileRDD {
    name: String,
}

impl RDD<String> for TextFileRDD {
    fn next() -> Option<String> {
        None
    }
}

#[test] 
fn test() {
    let fc = FlareContext {};
    let rdd = fc.textFile("./foo.txt".to_owned());

}

