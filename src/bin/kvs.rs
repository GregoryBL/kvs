extern crate clap;
use clap::{Arg, App, SubCommand};
// use kvs::KvStore;
use std::process;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(SubCommand::with_name("set")
                    .about("Set a key value pair.")
                    .arg(Arg::with_name("key")
                         .help("The name you use to get it out.")
                         .required(true)
                         .index(1)
                    ).arg(Arg::with_name("value")
                          .help("The value associated with the key.")
                          .required(true)
                          .index(2)
                    )
        ).subcommand(SubCommand::with_name("get")
                     .about("Get a value for a key.")
                     .arg(Arg::with_name("key")
                          .help("The name you want the value for.")
                          .required(true)
                          .index(1)
                     )
        ).subcommand(SubCommand::with_name("rm")
                     .about("Remove a key.")
                     .arg(Arg::with_name("key")
                          .help("The key you want to remove.")
                          .required(true)
                          .index(1)
                     )
        ).get_matches();

    // let kvs = KvStore::new();

    // let key = matches.value_of("key").unwrap().to_string();

    match matches.subcommand_name() {
        Some("set") => {
            // let value = matches.value_of("value").unwrap().to_string();
            // kvs.set(key,value);
            eprintln!("unimplemented");
            process::exit(1)
        },
        Some("get") => {
            // kvs.get(key);
            eprintln!("unimplemented");
            process::exit(1)
        },
        Some("rm")  => {
            // kvs.remove(key);
            eprintln!("unimplemented");
            process::exit(1)
        },
        _           => {
            eprintln!("Error: No subcommand match.");
            process::exit(1)
        },
    }
}
