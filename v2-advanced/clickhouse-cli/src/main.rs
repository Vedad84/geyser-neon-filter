mod db;
mod parse;

use core::panic;

use clap::{Arg, Command};
use clickhouse::{error::Result, Client};
use inquire::CustomType;
use inquire::{InquireError, Select};
use parse::SlotOrHash;

use crate::db::fetch_block_info;

async fn run_interactive(client: &Client) {
    let options: Vec<&str> = vec![
        "get_block_info",
        "get_transaction_info",
        "get_update_account",
        "get_update_slot",
    ];

    let ans: Result<&str, InquireError> = Select::new("Choose the command", options).prompt();

    match ans {
        Ok(choice) => match choice {
            "get_block_info" => {
                let answer =
                    CustomType::<SlotOrHash>::new("Enter slot or hash to receive block json:")
                        .with_error_message("Please type a valid u64 value or bs58 string.")
                        .prompt();

                if let Ok(slot_or_hash) = answer {
                    let result =
                        fetch_block_info(client, slot_or_hash.slot, slot_or_hash.hash).await;
                    match result {
                        Ok(block_info) => {
                            let prettified_json =
                                serde_json::to_string_pretty(&block_info).unwrap();
                            println!("{:?}", prettified_json);
                        }
                        Err(e) => panic!("Failed to execute get_block_info, error: {e}"),
                    }
                }
            }

            "get_transaction_info" => unimplemented!(),
            "get_update_account" => unimplemented!(),
            "get_update_slot" => unimplemented!(),

            &_ => panic!("Wrong command"),
        },
        Err(_) => println!("There was an error, please try again"),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let app = Command::new("geyser-neon-filter")
        .version("1.0")
        .about("Neonlabs filtering service")
        .arg(
            Arg::new("interactive")
                .default_value("true")
                .default_missing_value("true")
                .short('i')
                .required(false)
                .long("interactive")
                .value_parser(clap::value_parser!(bool))
                .help("Run in interactive mode"),
        )
        .get_matches();

    let client = Client::default().with_url("http://localhost:8123");

    if app.get_one::<bool>("interactive").is_some() {
        println!("Starting interactive mode...");
        run_interactive(&client).await;
    }

    Ok(())
}
