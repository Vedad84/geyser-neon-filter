mod db;
mod parse;

use core::panic;

use clap::{Arg, Command};
use clickhouse::{error::Result, Client};
use db::fetch_update_account;
use inquire::CustomType;
use inquire::Select;
use parse::{SlotOrHash, SlotOrSignature, VersionOrSignature};

use crate::db::fetch_update_slot;
use crate::db::{fetch_block_info, fetch_transaction_info};

async fn run_interactive(client: &Client) {
    let options: Vec<&str> = vec![
        "get_block_info",
        "get_transaction_info",
        "get_update_account",
        "get_update_slot",
    ];
    if let Ok(choice) = Select::new("Choose the command", options).prompt() {
        match choice {
            "get_block_info" => {
                if let Ok(soh) =
                    CustomType::<SlotOrHash>::new("Enter slot or hash to receive block json:")
                        .with_error_message("Please type a valid u64 value or bs58 string.")
                        .prompt()
                {
                    let prettified_json = match fetch_block_info(client, soh.slot, &soh.hash).await {
                        Ok(block_info) => serde_json::to_string_pretty(&block_info).expect("fetch_block_info to_string_pretty is failed"),
                        Err(e) => panic!("Failed to execute get_block_info with slot {:?} and hash {:?}, error: {e}", soh.slot, soh.hash),
                    };
                    println!("{}", prettified_json);
                }
            }
            "get_transaction_info" => {
                if let Ok(sos) = CustomType::<SlotOrSignature>::new(
                    "Enter slot or signature to receive transaction_info json:",
                )
                .with_error_message("Please type a valid u64 value or signature array.")
                .prompt()
                {
                    let prettified_json = match fetch_transaction_info(client, sos.slot, &sos.pubkey).await {
                        Ok(block_info) => serde_json::to_string_pretty(&block_info).expect("get_transaction_info to_string_pretty is failed"),
                        Err(e) => panic!("Failed to execute get_transaction_info with slot {:?} and signature {:?}, error: {e}", sos.slot, sos.pubkey),
                    };
                    println!("{}", prettified_json);
                }
            }
            "get_update_account" => {
                if let Ok(vos) = CustomType::<VersionOrSignature>::new(
                    "Enter write_version or pubkey to receive update_account_info:",
                )
                .with_error_message("Please type a valid write_version value or signature array.")
                .prompt()
                {
                    let prettified_json = match fetch_update_account(client, vos.write_version, &vos.signature).await {
                        Ok(block_info) => serde_json::to_string_pretty(&block_info).expect("get_transaction_info to_string_pretty is failed"),
                        Err(e) => panic!("Failed to execute get_update_account with write_version {:?} and signature {:?}, error: {e}", vos.write_version, vos.signature),
                    };
                    println!("{}", prettified_json);
                }
            }
            "get_update_slot" => {
                if let Ok(slot) =
                    CustomType::<u64>::new("Enter slot number to receive update_account_info:")
                        .with_error_message("Please type a valid slot value.")
                        .prompt()
                {
                    let prettified_json = match fetch_update_slot(client, slot).await {
                        Ok(block_info) => serde_json::to_string_pretty(&block_info)
                            .expect("get_transaction_info to_string_pretty is failed"),
                        Err(e) => panic!(
                            "Failed to execute get_update_slot with slot {:?} error: {e}",
                            slot
                        ),
                    };
                    println!("{}", prettified_json);
                }
            }

            &_ => panic!("Wrong command"),
        }
    } else {
        println!("There was an error, please try again");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let app = Command::new("clickhouse-cli")
        .version("1.0")
        .about("Neonlabs cli utility")
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
