mod db;
mod parse;

use core::panic;

use clap::{Arg, Command};
use clickhouse::{error::Result, Client};

use inquire::CustomType;
use inquire::Select;
use parse::{SlotOrHash, SlotOrSignature, VersionOrPubkey};

use crate::db::fetch_row_count;
use crate::db::fetch_table_info;
use crate::db::fetch_update_account;
use crate::db::fetch_update_slot;
use crate::db::{fetch_block_info, fetch_transaction_info};

async fn run_interactive(client: &Client) {
    let options: Vec<&str> = vec![
        "get_block_info",
        "get_transaction_info",
        "get_update_account",
        "get_update_slot",
        "get_info_for_table",
        "get_table_row_count",
    ];
    if let Ok(choice) = Select::new("Choose the command", options).prompt() {
        match choice {
            "get_block_info" => {
                if let Ok(soh) =
                    CustomType::<SlotOrHash>::new("Enter either the slot or the hash to receive block json:")
                        .with_error_message("Please type a valid u64 value or bs58 string.")
                        .prompt()
                {
                    let prettified_json = match fetch_block_info(client, &soh).await {
                        Ok(block_info) => serde_json::to_string_pretty(&block_info)
                            .expect("fetch_block_info to_string_pretty is failed"),
                        Err(e) => panic!(
                            "Failed to execute get_block_info with data {}, error: {e}",
                            soh
                        ),
                    };
                    println!("{prettified_json}");
                }
            }
            "get_transaction_info" => {
                if let Ok(sos) = CustomType::<SlotOrSignature>::new(
                    "Enter either the slot or the signature (in bs58 or plain array format) to receive transaction_info json:",
                )
                .with_error_message("Please type a valid u64 value or signature array.")
                .prompt()
                {
                    let prettified_json = match fetch_transaction_info(client, &sos).await {
                        Ok(block_info) => serde_json::to_string_pretty(&block_info).expect("get_transaction_info to_string_pretty is failed"),
                        Err(e) => panic!("Failed to execute get_transaction_info with slot {:?} and signature {:?}, error: {e}", sos.slot, sos.signature),
                    };
                    println!("{prettified_json}");
                }
            }

            "get_update_account" => {
                if let Ok(vop) = CustomType::<VersionOrPubkey>::new(
                    "Enter either the write_version or the pubkey (in bs58 or plain array format) to receive update_account_info:",
                )
                .with_error_message("Please type a valid write_version value or signature array.")
                .prompt()
                {
                    let prettified_json = match fetch_update_account(client, &vop).await {
                        Ok(block_info) => serde_json::to_string_pretty(&block_info)
                            .expect("get_transaction_info to_string_pretty is failed"),
                        Err(e) => panic!(
                            "Failed to execute get_update_account with data {}, error: {e}",
                            vop
                        ),
                    };
                    println!("{prettified_json}");
                }
            }

            "get_update_slot" => {
                if let Ok(slot) =
                    CustomType::<u64>::new("Enter the slot number to receive update_account_info:")
                        .with_error_message("Please type a valid slot value.")
                        .prompt()
                {
                    let prettified_json = match fetch_update_slot(client, slot).await {
                        Ok(block_info) => serde_json::to_string_pretty(&block_info)
                            .expect("get_update_slot to_string_pretty is failed"),
                        Err(e) => panic!(
                            "Failed to execute get_update_slot with slot {:?} error: {e}",
                            slot
                        ),
                    };
                    println!("{prettified_json}");
                }
            }

            "get_info_for_table" => {
                if let Ok(table_name) =
                    CustomType::<String>::new("Enter the table name to receive table information:")
                        .with_error_message("Please type a valid table name.")
                        .prompt()
                {
                    let prettified_json = match fetch_table_info(client, &table_name).await {
                        Ok(block_info) => serde_json::to_string_pretty(&block_info)
                            .expect("get_info_for_table to_string_pretty is failed"),
                        Err(e) => panic!(
                            "Failed to execute get_info_for_table with table_name {} error: {e}",
                            table_name
                        ),
                    };
                    println!("{prettified_json}");
                }
            }

            "get_table_row_count" => {
                if let Ok(table_name) =
                    CustomType::<String>::new("Enter the table name to receive count of rows:")
                        .with_error_message("Please type a valid table name.")
                        .prompt()
                {
                    let prettified_json = match fetch_row_count(client, &table_name).await {
                        Ok(block_info) => serde_json::to_string_pretty(&block_info)
                            .expect("get_table_row_count to_string_pretty is failed"),
                        Err(e) => panic!(
                            "Failed to execute get_table_row_count with table_name {} error: {e}",
                            table_name
                        ),
                    };
                    println!("{prettified_json}");
                }
            }

            &_ => panic!("Wrong command"),
        }
    } else {
        println!("There was an error, please try again");
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let app = Command::new("clickhouse-cli")
        .version("1.0")
        .about("Neonlabs cli utility")
        .arg(
            Arg::new("interactive")
                .short('i')
                .required(false)
                .long("interactive")
                .value_parser(clap::value_parser!(bool))
                .help("Run in interactive mode"),
        )
        .arg(
            Arg::new("get_row_count")
                .short('g')
                .required(false)
                .long("get_row_count")
                .value_parser(clap::value_parser!(String))
                .help("Get row count for specific table"),
        )
        .arg(
            Arg::new("server")
                .short('s')
                .required(true)
                .long("server")
                .value_parser(clap::value_parser!(String))
                .help("ClickHouse server:port to connect"),
        )
        .get_matches();

    if let Some(server) = app.get_one::<String>("server") {
        let client = Client::default().with_url(server);

        if app.get_one::<bool>("interactive").is_some() {
            println!("Starting interactive mode...");
            run_interactive(&client).await;
        }

        if let Some(table_name) = app.get_one::<String>("get_row_count") {
            match fetch_row_count(&client, table_name).await {
                Ok(trc) => {
                    println!("{}", trc.row_count);
                }
                Err(e) => {
                    panic!("Failed to execute get_row_count, error: {e}");
                }
            }
        }
    }

    Ok(())
}
