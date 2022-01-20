// #![deny(warnings)]
#![warn(unused_extern_crates)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::unreachable)]
#![deny(clippy::await_holding_lock)]
#![deny(clippy::needless_pass_by_value)]
#![deny(clippy::trivially_copy_pass_by_ref)]

use structopt::StructOpt;
use tracing::debug;
use std::process::Command;


#[derive(Debug, StructOpt)]
struct ListOpt {
    pool: String,
    #[structopt(short = "n")]
    dryrun: bool
}

#[derive(Debug, StructOpt)]
struct ReplOpt {
    from_pool: String,
    to_pool: String,
    #[structopt(short = "n")]
    dryrun: bool
}

#[derive(Debug, StructOpt)]
enum Action {
    #[structopt(name = "list_snapshots")]
    List(ListOpt),
    #[structopt(name = "init_repl")]
    Init(ReplOpt),
    #[structopt(name = "repl")]
    Repl(ReplOpt),
}

fn do_list(opt: &ListOpt) {
    let result = Command::new("zfs")
        .arg("list")
        .arg("-t")
        .arg("snapshot")
        .arg(opt.pool.as_str())
        .output();

    match result {
        Ok(output) => {
            debug!("{:?}", output);
        }
        Err(e) => {
            error!("snapshot list failed -> {:?}", e);
        }
    }
}

// https://doc.rust-lang.org/std/process/struct.Stdio.html#impl-From%3CChildStdout%3E

fn main() {
    tracing_subscriber::fmt::init();

    let opt = Action::from_args();

    debug!(?opt);

    match opt {
        Action::List(opt) => {
            do_list(&opt)
        }
        Action::Init(opt) => {
        }
        Action::Repl(opt) => {
        }
    }
}
