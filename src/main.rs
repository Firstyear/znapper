#![deny(warnings)]
#![warn(unused_extern_crates)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::unreachable)]
#![deny(clippy::await_holding_lock)]
#![deny(clippy::needless_pass_by_value)]
#![deny(clippy::trivially_copy_pass_by_ref)]

use std::fs::File;
use std::process::{Command, Stdio};
use structopt::StructOpt;
use time::OffsetDateTime;
use tracing::{debug, error, info, warn};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use serde::{Deserialize, Serialize};

use std::io;

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
struct InitArchiveOpt {
    pool: String,
    file: String,
    /// Path to a json metadata to track which autosnaps we are anchoring from
    auto_snap_metadata: String,
    #[structopt(short = "n")]
    dryrun: bool,
}

#[derive(Debug, StructOpt)]
struct ArchiveOpt {
    pool: String,
    file: String,
    #[structopt(short = "n")]
    dryrun: bool,
}

#[derive(Debug, StructOpt)]
struct ReplRemoteOpt {
    remote_ssh: String,
    /// Path to a json metadata to track which autosnaps we are anchoring from
    auto_snap_metadata: String,
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

    #[structopt(name = "remote_init_archive")]
    InitArchive(InitArchiveOpt),
    #[structopt(name = "remote_load_archive")]
    LoadArchive(ArchiveOpt),
    #[structopt(name = "remote_repl")]
    ReplRemote(ReplRemoteOpt),

    #[structopt(name = "snapshot")]
    Snapshot(Opt),
    #[structopt(name = "snapshot_cleanup")]
    SnapshotCleanup(CleanupOpt),
}

#[derive(Serialize, Deserialize)]
struct RemoteMetadata {
    precursor_snap: String,
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

fn snap_list(pool_name: &str, recurse: bool) -> Result<Vec<String>, ()> {
    let cmd = if recurse {
        Command::new("zfs")
            .arg("list")
            .arg("-H")
            .arg("-t")
            .arg("snapshot")
            .arg("-o")
            .arg("name")
            .arg("-r")
            .arg(pool_name)
            .output()
    } else {
        Command::new("zfs")
            .arg("list")
            .arg("-H")
            .arg("-t")
            .arg("snapshot")
            .arg("-o")
            .arg("name")
            .arg(pool_name)
            .output()
    };

    let stdout = cmd
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

fn filter_snap_list(filter: &str, pool_name: &str, recurse: bool) -> Result<Vec<String>, ()> {
    let snaps = snap_list(pool_name, recurse)?;
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
    filter_snap_list("repl_", pool_name, true)
}

fn auto_snap_list(pool_name: &str) -> Result<Vec<String>, ()> {
    filter_snap_list("auto_", pool_name, true)
}

fn do_list(opt: &ListOpt) {
    if let Ok(names) = snap_list(opt.pool.as_str(), true) {
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
     * -w for encyrption to stay raw. Is that needed locally?
     */
    if opt.dryrun {
        info!(
            "dryrun -> zfs send -v -R -w -L {} | zfs recv -o mountpoint=none -o readonly=on {}",
            basesnap_name, opt.to_pool
        );
    } else {
        let send = Command::new("zfs")
            .arg("send")
            .arg("-v")
            .arg("-R")
            .arg("-w")
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
     * Remove any holds/previous snaps from previous repls
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
    // zfs send -R -h -L nvme@snap1 | zfs recv -o mountpoint=none -o readonly=on tank/nvme

    /*
     * Remove any holds/previous snaps from previous repls on source and dest
     */
    if do_repl_inner(opt, &precursor_name, &basesnap_name).is_err() {
        info!("Removing potentially un-sent snapshot");
        let _ = remove_snap(opt.dryrun, basesnap_name.as_str());
        return;
    }

    debug!("Available Repl Snaps -> {:?}", from_snaps);
    for leftover_snap in from_snaps {
        let _ = remove_snap(opt.dryrun, leftover_snap.as_str());
    }
    debug!("Available Repl Snaps -> {:?}", to_snaps);
    for leftover_snap in to_snaps {
        let _ = remove_snap(opt.dryrun, leftover_snap.as_str());
    }
}

fn do_repl_inner(opt: &ReplOpt, precursor_name: &str, basesnap_name: &str) -> Result<(), ()> {
    if opt.dryrun {
        info!(
            "dryrun -> zfs send -v -R -w -L -I {} {} | zfs recv -o mountpoint=none -o readonly=on {}",
            precursor_name, basesnap_name, opt.to_pool
        );
        Ok(())
    } else {
        debug!(
            "running -> zfs send -v -R -w -L -I {} {} | zfs recv -o mountpoint=none -o readonly=on {}",
            precursor_name, basesnap_name, opt.to_pool
        );
        let send = Command::new("zfs")
            .arg("send")
            .arg("-v")
            .arg("-R")
            .arg("-w")
            .arg("-L")
            .arg("-I")
            .arg(precursor_name)
            .arg(basesnap_name)
            .stdout(Stdio::piped())
            .spawn();

        let mut send = match send {
            Ok(send) => send,
            Err(e) => {
                error!("send failed -> {:?}", e);
                return Err(());
            }
        };

        let recv = Command::new("zfs")
            .arg("recv")
            .arg("-o")
            .arg("mountpoint=none")
            .arg("-o")
            .arg("readonly=on")
            .arg(opt.to_pool.as_str())
            .stdin(send.stdout.take().unwrap())
            .status();

        match recv {
            Ok(status) => {
                let code = status.code().unwrap_or(255);
                if code == 0 {
                    warn!("success recv code {}", code);
                    // Happy path.
                } else {
                    error!("recv code {}", code);
                    return Err(());
                }
            }
            Err(e) => {
                error!("ssh recv failed -> {:?}", e);
                return Err(());
            }
        };

        match send.wait() {
            Ok(status) => {
                if !status.success() {
                    error!("send failed");
                    return Err(());
                }
                // Happy path.
            }
            Err(e) => {
                error!("send failed -> {:?}", e);
                return Err(());
            }
        };

        info!("Incremental replication success");
        Ok(())
    }
}

fn get_auto_basesnap(pool_name: &str) -> Option<String> {
    let snaps: Vec<_> = filter_snap_list("auto_", pool_name, true).ok()?;

    // Find the "latest" autosnap.
    snaps
        .iter()
        .last()
        .and_then(|snap| snap.rsplit("@").map(str::to_string).next())
}

fn do_init_archive(opt: &InitArchiveOpt) {
    debug!("do_init_archive");

    let basesnap_name = match get_auto_basesnap(&opt.pool) {
        Some(b) => b,
        None => {
            error!("No auto-snaps available");
            return;
        }
    };

    /*
     * do the send/recv
     * -w for encyrption to stay raw
     */
    if opt.dryrun {
        info!(
            "dryrun -> zfs send -v -R -L -w {} > {}",
            basesnap_name, opt.file
        );
    } else {
        let meta = match File::create(&opt.auto_snap_metadata) {
            Ok(f) => f,
            Err(e) => {
                error!("failed to open file -> {:?}", e);
                return;
            }
        };

        if let Err(e) = serde_json::to_writer(
            &meta,
            &RemoteMetadata {
                precursor_snap: basesnap_name.clone(),
            },
        ) {
            error!("failed to write metadata file -> {:?}", e);
            return;
        }

        let mut file = match File::create(&opt.file) {
            Ok(f) => f,
            Err(e) => {
                error!("failed to open file -> {:?}", e);
                return;
            }
        };

        let send = Command::new("zfs")
            .arg("send")
            .arg("-v")
            .arg("-R")
            .arg("-L")
            .arg("-w")
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

        let mut stdout = match send.stdout.take() {
            Some(s) => s,
            None => {
                error!("Failed to connect to stdout of zfs send process");
                return;
            }
        };

        match io::copy(&mut stdout, &mut file) {
            Ok(b) => debug!("wrote {} bytes", b),
            Err(e) => {
                error!("Failed to write to file -> {:?}", e);
            }
        };

        if let Err(e) = send.wait() {
            error!("send failed -> {:?}", e);
            return;
        } else {
            info!("Initial replication archive success")
        }
    }
}

fn do_load_archive(opt: &ArchiveOpt) {
    debug!("do_load_archive");

    if opt.dryrun {
        info!(
            "dryrun -> cat {} | zfs recv -o mountpoint=none -o readonly=on {}",
            opt.file, opt.pool
        );
    } else {
        let mut file = match File::open(&opt.file) {
            Ok(f) => f,
            Err(e) => {
                error!("failed to open file -> {:?}", e);
                return;
            }
        };

        let recv = Command::new("zfs")
            .arg("recv")
            .arg("-o")
            .arg("mountpoint=none")
            .arg("-o")
            .arg("readonly=on")
            .arg(opt.pool.as_str())
            .stdin(Stdio::piped())
            .spawn();

        let mut recv = match recv {
            Ok(recv) => recv,
            Err(e) => {
                error!("recv failed -> {:?}", e);
                return;
            }
        };

        let mut stdin = match recv.stdin.take() {
            Some(s) => s,
            None => {
                error!("Failed to connect to stdin of zfs recv process");
                return;
            }
        };

        match io::copy(&mut file, &mut stdin) {
            Ok(b) => debug!("wrote {} bytes", b),
            Err(e) => {
                error!("Failed to write to zfs recv -> {:?}", e);
            }
        };

        if let Err(e) = recv.wait() {
            error!("recv failed -> {:?}", e);
            return;
        } else {
            info!("Initial replication archive load success");
            warn!("You should now setup a remote backup user. For that user in .ssh/authorized_keys set:");
            warn!(
                r#"  command="/usr/sbin/zfs recv -x mountpoint -x readonly {}",no-port-forwarding,no-X11-forwarding,no-agent-forwarding,no-pty [ssh-key]"#,
                opt.pool
            );
            warn!("You must also setup permission delegation for that user to recv replication snapshots");
            warn!("  zfs allow [user] mount,create,receive {}", opt.pool);
        }
    }
}

fn do_repl_remote(opt: &ReplRemoteOpt) {
    debug!("do_repl_remote");

    /*
     * If you get:
     *  cannot receive incremental stream: most recent snapshot of tank/remote does not
     *  match incremental source
     *
     * ZFS wants to send from the 'latest' the remote knows about. You can't repeat-send
     * snapshot. So let say the source has:
     *
     * NAME                                  USED  AVAIL     REFER  MOUNTPOINT
     * tank@remote_2022_05_22_12_09_46         0B      -      100K  -
     * tank@remote_2022_05_22_12_11_24         0B      -      100K  -
     * tank@remote_2022_05_22_12_11_51         0B      -      100K  -
     * tank@remote_2022_05_22_12_12_59         0B      -      100K  -
     *
     * And the remote has:
     *
     * NAME                                  USED  AVAIL     REFER  MOUNTPOINT
     * tank/remote@remote_2022_05_22_12_09_46         0B      -      100K  -
     * tank/remote@remote_2022_05_22_12_11_24         0B      -      100K  -
     *
     * And we issued:
     *
     * zfs send -R -L -w -I tank@remote_2022_05_22_12_09_46 tank@remote_2022_05_22_12_12_59
     * Because 09_46 is not the "latest" on remote, that's why it won't apply. We needed to anchor
     * from 11_24 instead.
     *
     */

    /*
     * The problem we have is that we can't tell the difference between a success and failure, so
     * it can be fragile to work this out :(
     *
     * That's why we only "keep" a snapshot for repl_ IF it appears everything succeedd, but it's
     * still not perfect, and will need monitoring :(
     */

    // Get the precursor snap from the metadata
    let meta: RemoteMetadata = match File::open(&opt.auto_snap_metadata)
        .map_err(|e| {
            error!("Failed to open metadata file {:?}", e);
            ()
        })
        .and_then(|f| {
            serde_json::from_reader(f).map_err(|e| {
                error!("Failed to parse metadata file {:?}", e);
                ()
            })
        }) {
        Ok(p) => p,
        Err(_) => return,
    };

    let precursor_name = meta.precursor_snap;

    let pool = precursor_name.split('@').next().unwrap();

    // get the new base snap from the latest auto.
    let basesnap_name = match get_auto_basesnap(pool) {
        Some(b) => b,
        None => {
            error!("No auto-snaps available");
            return;
        }
    };

    /*
     * Remove any holds/previous snaps from previous repls on source and dest
     */

    if opt.dryrun {
        info!(
            "dryrun -> zfs send -v -R -L -w -I {} {} | ssh {}",
            precursor_name, basesnap_name, opt.remote_ssh
        );
        return;
    } else {
        debug!(
            "running -> zfs send -v -R -L -w -I {} {} | ssh {}",
            precursor_name, basesnap_name, opt.remote_ssh
        );

        let send = Command::new("zfs")
            .arg("send")
            .arg("-v")
            .arg("-R")
            .arg("-L")
            .arg("-w")
            .arg("-I")
            .arg(precursor_name)
            .arg(basesnap_name)
            .stdout(Stdio::piped())
            .spawn();

        let mut send = match send {
            Ok(send) => send,
            Err(e) => {
                error!("send failed -> {:?}", e);
                return;
            }
        };

        let recv = Command::new("ssh")
            .arg(opt.remote_ssh.as_str())
            .stdin(send.stdout.take().unwrap())
            .status();

        match recv {
            Ok(status) => {
                let code = status.code().unwrap_or(255);
                if code == 1 || code == 0 {
                    warn!("success recv code {}", code);
                    // Happy path.
                } else {
                    error!("recv code {}", code);
                    return;
                }
            }
            Err(e) => {
                error!("ssh recv failed -> {:?}", e);
                return;
            }
        };

        match send.wait() {
            Ok(status) => {
                if !status.success() {
                    error!("send failed");
                    return;
                }
                // Happy path.
            }
            Err(e) => {
                error!("send failed -> {:?}", e);
                return;
            }
        };

        info!("Incremental remote replication success");
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
        Action::InitArchive(opt) => do_init_archive(&opt),
        Action::LoadArchive(opt) => do_load_archive(&opt),
        Action::ReplRemote(opt) => do_repl_remote(&opt),
        Action::Snapshot(opt) => do_snap(&opt),
        Action::SnapshotCleanup(opt) => do_snap_cleanup(&opt),
    }
}
