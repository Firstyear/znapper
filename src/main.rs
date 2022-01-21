#![deny(warnings)]
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
use tracing::{debug, error, info, warn};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short = "n")]
    dryrun: bool,
}

#[derive(Debug, StructOpt)]
struct ListOpt {
    pool: String,
    // #[structopt(short = "n")]
    // dryrun: bool,
}

#[derive(Debug, StructOpt)]
struct CleanupOpt {
    pool: String,
    keep_hours: u32,
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
    #[structopt(name = "snapshot")]
    Snapshot(Opt),
    #[structopt(name = "snapshot_cleanup")]
    SnapshotCleanup(CleanupOpt),
}

fn mounted_list() -> Result<Vec<String>, ()> {
    let stdout = Command::new("zfs")
        .arg("list")
        .arg("-H")
        .arg("-t")
        .arg("filesystem")
        .arg("-o")
        .arg("name,mountpoint")
        .output()
        .map_err(|e| {
            error!("mounted list failed -> {:?}", e);
        })
        .and_then(|output| {
            String::from_utf8(output.stdout).map_err(|e| {
                error!("mounted list contains invalid utf8 -> {:?}", e);
            })
        })?;

    let lines: Vec<_> = stdout.split("\n").collect();
    debug!("{:?}", lines);

    Ok(lines
        .iter()
        .filter_map(|line| {
            let mut lsplit = line.split_whitespace();
            match (lsplit.next(), lsplit.next()) {
                (Some(_), Some("none")) => None,
                (Some(name), Some(_)) => Some(name),
                _ => None,
            }
        })
        .map(str::to_string)
        .collect())
}

fn snap_list(pool_name: &str) -> Result<Vec<String>, ()> {
    let stdout = Command::new("zfs")
        .arg("list")
        .arg("-H")
        .arg("-t")
        .arg("snapshot")
        .arg("-o")
        .arg("name")
        .arg("-r")
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

    let lines: Vec<_> = stdout.split("\n").map(str::to_string).collect();
    debug!("{:?}", lines);
    Ok(lines)
}

fn filter_snap_list(filter: &str, pool_name: &str) -> Result<Vec<String>, ()> {
    let snaps = snap_list(pool_name)?;
    let mut snaps: Vec<_> = snaps
        .into_iter()
        .filter_map(|snap| {
            if snap
                .rsplit("@")
                .next()
                .map(|name| name.starts_with(filter))
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

fn repl_snap_list(pool_name: &str) -> Result<Vec<String>, ()> {
    filter_snap_list("repl_", pool_name)
}

fn auto_snap_list(pool_name: &str) -> Result<Vec<String>, ()> {
    filter_snap_list("auto_", pool_name)
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
        info!("remove_snap -> {}", snap_name);
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
        info!("create_snap -> {}", snap_name);
        Command::new("zfs")
            .arg("snapshot")
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

fn create_recurse_snap(dry: bool, snap_name: &str) -> Result<(), ()> {
    if dry {
        info!("dryrun: create_recurse_snap -> {}", snap_name);
        Ok(())
    } else {
        info!("create_recurse_snap -> {}", snap_name);
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

fn do_snap(opt: &Opt) {
    let mounted: Vec<_> = match mounted_list() {
        Ok(fs) => fs,
        Err(_) => {
            return;
        }
    };

    let now_ts = match OffsetDateTime::try_now_local() {
        Ok(t) => t.format("%Y_%m_%d_%H_%M_%S"),
        Err(_) => {
            error!("Unable to determine time");
            return;
        }
    };

    for fs in mounted.iter() {
        let snap_name = format!("{}@auto_{}", fs, now_ts);
        if create_snap(opt.dryrun, snap_name.as_str()).is_err() {
            warn!("Failed to create snapshot -> {}", snap_name);
        }
    }
}

fn do_snap_cleanup(opt: &CleanupOpt) {
    let dur = time::Duration::hours(opt.keep_hours as i64);
    let now_ts = match OffsetDateTime::try_now_local() {
        Ok(t) => (t - dur).format("%Y_%m_%d_%H_%M_%S"),
        Err(_) => {
            error!("Unable to determine time");
            return;
        }
    };

    debug!("{:?}", now_ts);

    let snaps: Vec<_> = match auto_snap_list(opt.pool.as_str()) {
        Ok(snaps) => snaps,
        Err(_) => {
            return;
        }
    };

    let up_to_ts = format!("auto_{}", now_ts);

    let remove_snaps: Vec<_> = snaps
        .into_iter()
        .filter(|snap_name| {
            if let Some(n) = snap_name.rsplit("@").next() {
                n.starts_with("auto_") && n < up_to_ts.as_str()
            } else {
                false
            }
        })
        .collect();

    debug!("would remove -> {:?}", remove_snaps);

    for snap in remove_snaps {
        let _ = remove_snap(opt.dryrun, snap.as_str());
    }
}

fn do_init(opt: &ReplOpt) {
    debug!("do_init");

    let now_ts = match OffsetDateTime::try_now_local() {
        Ok(t) => t.format("%Y_%m_%d_%H_%M_%S"),
        Err(_) => {
            error!("Unable to determine time");
            return;
        }
    };

    debug!("{:?}", now_ts);

    let snaps: Vec<_> = match repl_snap_list(opt.from_pool.as_str()) {
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

    if create_recurse_snap(opt.dryrun, basesnap_name.as_str()).is_err() {
        return;
    }

    /*
     * do the send/recv
     */
    if opt.dryrun {
        info!(
            "dryrun -> zfs send -R -L {} | zfs recv -F -o mountpoint=none -o readonly=true {}",
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

        let mut send = match send {
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
            .arg("-o")
            .arg("readonly=on")
            .arg(opt.to_pool.as_str())
            .stdin(send.stdout.take().unwrap())
            .status();

        if let Err(e) = recv {
            error!("recv failed -> {:?}", e);
            return;
        } else if let Err(e) = send.wait() {
            error!("send failed -> {:?}", e);
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
        let _ = remove_snap(opt.dryrun, leftover_snap.as_str());
    }
}

fn do_repl(opt: &ReplOpt) {
    debug!("do_repl");

    let now_ts = match OffsetDateTime::try_now_local() {
        Ok(t) => t.format("%Y_%m_%d_%H_%M_%S"),
        Err(_) => {
            error!("Unable to determine time");
            return;
        }
    };

    let from_snaps: Vec<_> = match repl_snap_list(opt.from_pool.as_str()) {
        Ok(snaps) => snaps,
        Err(_) => {
            return;
        }
    };

    let to_snaps: Vec<_> = match repl_snap_list(opt.to_pool.as_str()) {
        Ok(snaps) => snaps,
        Err(_) => {
            return;
        }
    };

    // What is the precursor snap? We remove it from the set of cleanup snaps.
    let precursor_name = match from_snaps
        .iter()
        .rev()
        .filter_map(|from_snap| {
            // Is it in the to_snap?
            to_snaps
                .iter()
                .rev()
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
        .next()
    {
        Some(n) => n,
        None => {
            error!("No previous matching snaps available - you may need to restart repl");
            return;
        }
    };

    /*
     * Init a new repl snap
     */
    let basesnap_name = format!("{}@repl_{}", opt.from_pool, now_ts);
    if create_recurse_snap(opt.dryrun, basesnap_name.as_str()).is_err() {
        return;
    }

    /*
     * do the send/recv
     */
    // zfs send -R -h -L nvme@snap1 | zfs recv -F -o mountpoint=none -o readonly=true tank/nvme
    if opt.dryrun {
        info!(
            "dryrun -> zfs send -R -L -I {} {} | zfs recv -F -o mountpoint=none -o readonly=true {}",
            precursor_name, basesnap_name, opt.to_pool
        );
    } else {
        debug!(
            "running -> zfs send -R -L -I {} {} | zfs recv -F -o mountpoint=none -o readonly=true {}",
            precursor_name, basesnap_name, opt.to_pool
        );
        let send = Command::new("zfs")
            .arg("send")
            .arg("-R")
            .arg("-L")
            .arg("-I")
            .arg(precursor_name.as_str())
            .arg(basesnap_name.as_str())
            .stdout(Stdio::piped())
            .spawn();

        let mut send = match send {
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
            .arg("-o")
            .arg("readonly=on")
            .arg(opt.to_pool.as_str())
            .stdin(send.stdout.take().unwrap())
            .status();

        if let Err(e) = recv {
            error!("recv failed -> {:?}", e);
            return;
        } else if let Err(e) = send.wait() {
            error!("send failed -> {:?}", e);
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
        let _ = remove_snap(opt.dryrun, leftover_snap.as_str());
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
        Action::Repl(opt) => do_repl(&opt),
        Action::Snapshot(opt) => do_snap(&opt),
        Action::SnapshotCleanup(opt) => do_snap_cleanup(&opt),
    }
}
