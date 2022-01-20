// #![deny(warnings)]
#![warn(unused_extern_crates)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::unreachable)]
#![deny(clippy::await_holding_lock)]
#![deny(clippy::needless_pass_by_value)]
#![deny(clippy::trivially_copy_pass_by_ref)]

use std::process::{Command, Stdio};
use structopt::StructOpt;
use time::OffsetDateTime;
use tracing::{debug, error, info};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug, StructOpt)]
struct ListOpt {
    pool: String,
    #[structopt(short = "n")]
    dryrun: bool,
}

#[derive(Debug, StructOpt)]
struct ReplOpt {
    from_pool: String,
    to_pool: String,
    #[structopt(short = "n")]
    dryrun: bool,
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

fn snap_list(pool_name: &str) -> Result<Vec<String>, ()> {
    let stdout = Command::new("zfs")
        .arg("list")
        .arg("-t")
        .arg("snapshot")
        .arg(pool_name)
        .output()
        .map_err(|e| {
            error!("snapshot list failed -> {:?}", e);
        })
        .and_then(|output| {
            String::from_utf8(output.stdout).map_err(|e| {
                error!("snapshot list contains invalid utf8 -> {:?}", e);
            })
        })?;

    let lines: Vec<_> = stdout.split("\n").collect();
    debug!("{:?}", lines);

    // Skip the first, it's:
    // "NAME         USED  AVAIL     REFER  MOUNTPOINT"
    let mut liter = lines.iter();
    let _ = liter.next();

    Ok(liter
        .filter_map(|line| {
            if line.len() > 0 {
                line.split_ascii_whitespace().next()
            } else {
                None
            }
        })
        .map(str::to_string)
        .collect())
}

fn repl_snap_list(pool_name: &str) -> Result<Vec<String>, ()> {
    let snaps = snap_list(pool_name)?;
    let mut snaps: Vec<_> = snaps
            .into_iter()
            .filter_map(|snap| {
                if snap
                    .rsplit("@")
                    .next()
                    .map(|name| name.starts_with("repl_"))
                    .unwrap_or(false)
                {
                    Some(snap.clone())
                } else {
                    None
                }
            })
            .collect();
    snaps.sort_unstable();
    Ok(snaps)
}

fn do_list(opt: &ListOpt) {
    if let Ok(names) = snap_list(opt.pool.as_str()) {
        for name in names {
            info!("{}", name);
        }
    }
}

fn remove_snap(dry: bool, snap_name: &str) -> Result<(), ()> {
    if dry {
        info!("dryrun: remove_snap -> {}", snap_name);
        Ok(())
    } else {
        debug!("remove_snap -> {}", snap_name);
        Command::new("zfs")
            .arg("destroy")
            .arg("-r")
            .arg(snap_name)
            .status()
            .map_err(|e| {
                error!("snapshot remove failed -> {:?}", e);
            })
            .map(|status| {
                debug!(?status);
            })
    }
}

fn create_snap(dry: bool, snap_name: &str) -> Result<(), ()> {
    if dry {
        info!("dryrun: create_snap -> {}", snap_name);
        Ok(())
    } else {
        debug!("create_snap -> {}", snap_name);
        Command::new("zfs")
            .arg("snapshot")
            .arg("-r")
            .arg(snap_name)
            .status()
            .map_err(|e| {
                error!("snapshot create failed -> {:?}", e);
            })
            .map(|status| {
                debug!(?status);
            })
    }
}

fn do_init(opt: &ReplOpt) {
    debug!("do_init");

    let now_ts = match OffsetDateTime::try_now_local() {
        Ok(t) => t.format("%Y%m%d%H%M%S"),
        Err(e) => {
            error!("Unable to determine time");
            return;
        }
    };

    debug!("{:?}", now_ts);

    let mut snaps: Vec<_> = match repl_snap_list(opt.from_pool.as_str()) {
        Ok(snaps) => snaps,
        Err(_) => {
            return;
        }
    };

    /*
     * Init a base snap
     * Set the hold on the basesnap
     */
    let basesnap_name = format!("{}@repl_{}", opt.from_pool, now_ts);

    if create_snap(opt.dryrun, basesnap_name.as_str()).is_err() {
        return;
    }

    /*
     * do the send/recv
     */
    // zfs send -R -h -L nvme@snap1 | zfs recv -F -o mountpoint=none tank/nvme
    if opt.dryrun {
        info!(
            "dryrun -> zfs send -R -h -L {} | zfs recv -F -o mountpoint=none {}",
            basesnap_name, opt.to_pool
        );
    } else {
        let send = Command::new("zfs")
            .arg("send")
            .arg("-R")
            .arg("-L")
            .arg(basesnap_name.as_str())
            .stdout(Stdio::piped())
            .spawn();

        let send = match send {
            Ok(send) => send,
            Err(e) => {
                error!("send failed -> {:?}", e);
                return;
            }
        };

        let recv = Command::new("zfs")
            .arg("recv")
            .arg("-F")
            .arg("-o")
            .arg("mountpoint=none")
            .arg(opt.to_pool.as_str())
            .stdin(send.stdout.unwrap())
            .status();

        if let Err(e) = recv {
            error!("recv failed -> {:?}", e);
            return;
        } else {
            info!("Initial replication success")
        }
    }

    /*
     * Remove any holds/previous snaps from previous repls.
     */
    debug!("Available Repl Snaps -> {:?}", snaps);
    for leftover_snap in snaps {
        remove_snap(opt.dryrun, leftover_snap.as_str());
    }
}

fn do_repl(opt: &ReplOpt) {
    debug!("do_repl");

    let now_ts = match OffsetDateTime::try_now_local() {
        Ok(t) => t.format("%Y%m%d%H%M%S"),
        Err(e) => {
            error!("Unable to determine time");
            return;
        }
    };

    let mut from_snaps: Vec<_> = match repl_snap_list(opt.from_pool.as_str()) {
        Ok(snaps) => snaps,
        Err(_) => {
            return;
        }
    };

    let mut to_snaps: Vec<_> = match repl_snap_list(opt.to_pool.as_str()) {
        Ok(snaps) => snaps,
        Err(_) => {
            return;
        }
    };

    // What is the precursor snap? We remove it from the set of cleanup snaps.
    let precursor_name = match from_snaps.iter().rev()
        .filter_map(|from_snap| {
            // Is it in the to_snap?
            to_snaps.iter().rev()
                .filter_map(|to_snap| {
                    debug!("{} == {}", to_snap, from_snap);
                    if to_snap.ends_with(from_snap) {
                        Some(from_snap.clone())
                    } else {
                        None
                    }
                })
                .next()
        })
        .take(1)
        .next() {
        Some(n) => n,
        None => {
            error!("No previous matching snaps available - you may need to restart repl");
            return;
        }
    };

    /*
     * Init a new snap
     */
    let basesnap_name = format!("{}@repl_{}", opt.from_pool, now_ts);
    if create_snap(opt.dryrun, basesnap_name.as_str()).is_err() {
        return;
    }

    /*
     * do the send/recv
     */
    // zfs send -R -h -L nvme@snap1 | zfs recv -F -o mountpoint=none tank/nvme
    if opt.dryrun {
        info!(
            "dryrun -> zfs send -R -L -i {} {} | zfs recv -F -o mountpoint=none {}",
            precursor_name, basesnap_name, opt.to_pool
        );
    } else {
        debug!(
            "running -> zfs send -R -L -i {} {} | zfs recv -F -o mountpoint=none {}",
            precursor_name, basesnap_name, opt.to_pool
        );
        let send = Command::new("zfs")
            .arg("send")
            .arg("-R")
            .arg("-L")
            .arg("-i")
            .arg(precursor_name.as_str())
            .arg(basesnap_name.as_str())
            .stdout(Stdio::piped())
            .spawn();

        let send = match send {
            Ok(send) => send,
            Err(e) => {
                error!("send failed -> {:?}", e);
                return;
            }
        };

        let recv = Command::new("zfs")
            .arg("recv")
            .arg("-F")
            .arg("-o")
            .arg("mountpoint=none")
            .arg(opt.to_pool.as_str())
            .stdin(send.stdout.unwrap())
            .status();

        if let Err(e) = recv {
            error!("recv failed -> {:?}", e);
            return;
        } else {
            info!("Incremental replication success")
        }
    }

    /*
     * Remove any holds/previous snaps from previous repls.
     */
    debug!("Available Repl Snaps -> {:?}", from_snaps);
    for leftover_snap in from_snaps {
        remove_snap(opt.dryrun, leftover_snap.as_str());
    }
}

// https://doc.rust-lang.org/std/process/struct.Stdio.html#impl-From%3CChildStdout%3E

fn main() {
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();
    let fmt_layer = fmt::layer().with_target(false);

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let opt = Action::from_args();

    debug!(?opt);

    match opt {
        Action::List(opt) => do_list(&opt),
        Action::Init(opt) => do_init(&opt),
        Action::Repl(opt) => {
            do_repl(&opt)
        }
    }
}
