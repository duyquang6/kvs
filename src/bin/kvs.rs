use clap::{App, AppSettings, Arg, SubCommand};
use kvs::KvStore;
use std::process::exit;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("KEY").help("A string key").required(true))
                .arg(
                    Arg::with_name("VALUE")
                        .help("The string value of the key")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .get_matches();

    let mut store = KvStore::default();

    match matches.subcommand() {
        ("set", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap().to_string();
            let value = _matches.value_of("VALUE").unwrap().to_string();

            let result = store.set(key, value);
            if let Err(err) = result {
                eprintln!("{}", err);
                exit(1);
            } else {
                exit(0);
            }
        }
        ("get", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap().to_string();

            let result = store.get(key);
            match result {
                Ok(inner_opt) => {
                    if let Some(inner) = inner_opt {
                        println!("{}", inner);
                    } else {
                        println!("Key not found");
                    }
                    exit(0)
                }
                Err(err) => {
                    eprintln!("{}", err);
                    exit(1)
                }
            }
        }
        ("rm", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap().to_string();
            let result = store.remove(key);
            match result {
                Ok(_) => exit(0),
                Err(err) => {
                    println!("{}", err);
                    exit(1)
                }
            }
        }
        _ => unreachable!(),
    }
}
