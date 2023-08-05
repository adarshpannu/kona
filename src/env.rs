// env

use crate::{datum::Datum, includes::*, logging, metadata::Metadata, scheduler::Scheduler};

#[derive(Debug, Default)]
pub struct EnvSettings {
    pub parallel_degree: Option<usize>,
    pub parse_only: Option<bool>,
    pub trace: Option<String>,
}

pub struct Env {
    pub id: usize,
    pub scheduler: Scheduler,
    pub metadata: Metadata,
    pub input_pathname: String,
    pub output_dir: String,
    pub settings: EnvSettings,
}

impl Env {
    pub fn new(id: usize, nthreads: usize, input_pathname: String, output_dir: String) -> Self {
        let scheduler = Scheduler::new(nthreads);
        let metadata = Metadata::default();
        let options = EnvSettings::default();

        Env { id, scheduler, metadata, input_pathname, output_dir, settings: options }
    }

    pub fn set_option(&mut self, name: String, value: Datum) -> Result<(), String> {
        debug!("SET {} = {}", &name, &value);
        let name = name.to_uppercase();
        match name.as_str() {
            "PARALLEL_DEGREE" => self.settings.parallel_degree = Some(self.get_int_option(name.as_str(), &value)? as usize),
            "PARSE_ONLY" => self.settings.parse_only = Some(self.get_boolean_option(name.as_str(), &value)?),
            "TRACE" => {
                self.settings.trace = Some(self.get_string_option(name.as_str(), &value)?);
                logging::init(&self.settings.trace.as_ref().unwrap());
            }
            _ => return Err(f!("Invalid option specified: {name}.")),
        };
        Ok(())
    }

    pub fn get_boolean_option(&self, name: &str, value: &Datum) -> Result<bool, String> {
        if let Datum::STR(s) = value {
            let s = s.to_uppercase();
            return match s.as_str() {
                "TRUE" | "T" | "YES" | "Y" => Ok(true),
                _ => Ok(false),
            };
        }
        Err(f!("Option {name} needs to be a string. It holds {value} instead."))
    }

    pub fn get_int_option(&self, name: &str, value: &Datum) -> Result<isize, String> {
        if let Datum::INT(ival) = value {
            return Ok(*ival);
        }
        Err(f!("Option {name} needs to be an integer. It holds {value} instead."))
    }

    pub fn get_string_option(&self, name: &str, value: &Datum) -> Result<String, String> {
        if let Datum::STR(s) = value {
            Ok((&**s).clone())
        } else {
            Err(f!("Option {name} needs to be a string. It holds {value} instead."))
        }
    }
}
