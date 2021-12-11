
#[derive(Debug)]
pub enum FlareErrorCode {
    TableAlreadyCataloged,
}

#[derive(Debug)]
pub struct FlareError {
    code: FlareErrorCode,
    msg: String
}

impl FlareError {
    pub fn new(code: FlareErrorCode, msg: String) -> FlareError {
        FlareError { code, msg }
    }
}
