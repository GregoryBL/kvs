extern crate clap;
#[macro_use] extern crate failure;

use clap::{App, Arg, SubCommand, ArgMatches};
//use failure::err_msg;
use std::process;

use kvs::{KvStore, Result};
use std::path::Path;


fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("set")
                .about("Set a key value pair.")
                .arg(
                    Arg::with_name("key")
                        .help("The name you use to get it out.")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("value")
                        .help("The value associated with the key.")
                        .required(true)
                        .index(2),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get a value for a key.")
                .arg(
                    Arg::with_name("key")
                        .help("The name you want the value for.")
                        .required(true)
                        .index(1),
                ),
        )
        .subcommand(
            SubCommand::with_name("rm").about("Remove a key.").arg(
                Arg::with_name("key")
                    .help("The key you want to remove.")
                    .required(true)
                    .index(1),
            ),
        )
        .get_matches();

    let mut kvs = KvStore::open(Path::new("kvstore.store"))?;

    match matches.subcommand() {
        ("set", Some(sub_m)) => {
            let key = get_key(&sub_m)?;
            let value = get_value(&sub_m)?;
            kvs.set(key,value)
        }
        ("get", Some(sub_m)) => {
            let key = get_key(&sub_m)?;
            let val = kvs.get(key)?;
            println!("{}", val.unwrap_or("Key not found".to_string()));
            Ok(())
        }
        ("rm", Some(sub_m)) => {
            let key = get_key(&sub_m)?;
            match kvs.get(key.clone())? {
                None => {
                    println!("Key not found");
                    process::exit(1)
                },
                Some(_) => {
                    kvs.remove(key)
                }
            }
        }
        _ => {
            eprintln!("Error: No subcommand match.");
            process::exit(1)
        }
    }
}

fn get_key(matches: &ArgMatches) -> Result<String> {
    matches.value_of("key")
        .ok_or(format_err!("failed to find key in args"))
        .map(|e| e.to_string())
}

fn get_value(matches: &ArgMatches) -> Result<String> {
    matches.value_of("value")
        .ok_or(format_err!("failed to find value in args"))
        .map(|e| e.to_string())
}
