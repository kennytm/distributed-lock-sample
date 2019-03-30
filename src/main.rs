use std::thread::{sleep, spawn};
use std::time::{Duration, SystemTime};

use etcd_rs::prelude::*;
use etcd_rs::Client;

use futures::{
    sync::{mpsc, oneshot},
    Future, Sink, Stream,
};

fn spawn_keep_alive(client: &Client, lease_id: i64) {
    let lease_client = client.lease();
    let task = tokio::timer::Interval::new_interval(Duration::from_secs(1))
        .map_err(|_| ())
        .and_then(move |_| {
            lease_client
                .keep_alive_once(KeepAliveRequest::new(lease_id))
                .then(|r| match r {
                    Ok(resp) => {
                        if resp.ttl() == 0 {
                            println!("lease expired");
                            Err(())
                        } else {
                            Ok(())
                        }
                    }
                    Err(e) => {
                        println!("failed to keep alive: {:?}", e);
                        Err(())
                    }
                })
        })
        .for_each(|_| Ok(()));

    tokio::spawn(task);
}

enum LockTask {
    Lock(oneshot::Sender<Vec<u8>>),
    Unlock(Vec<u8>),
    Revoke,
}

const LOCK_NAME: &str = "tikv_importer/prepare_lock";

fn run_leaser(client: Client, prepare_lock_recv: mpsc::Receiver<LockTask>) {
    let least_client = client.lease();

    let task = least_client
        .grant(GrantRequest::new(5)) // <-- keep alive for 5 seconds.
        .map_err(|_| ())
        .and_then(move |resp| {
            let lease_id = resp.id();
            spawn_keep_alive(&client, lease_id);

            let lock_client = client.lock();

            prepare_lock_recv.for_each(move |task| match task {
                LockTask::Lock(reply) => Box::new(
                    lock_client
                        .lock(LockRequest::new(LOCK_NAME, lease_id))
                        .map(move |lock_resp| reply.send(lock_resp.key().to_owned()).unwrap())
                        .map_err(|e| eprintln!("lock failed: {:?}", e)),
                )
                    as Box<dyn Future<Item = (), Error = ()> + Send>,

                LockTask::Unlock(key) => Box::new(
                    lock_client
                        .unlock(UnlockRequest::new(key))
                        .map(|_| ())
                        .map_err(|e| eprintln!("unlock failed: {:?}", e)),
                )
                    as Box<dyn Future<Item = (), Error = ()> + Send>,

                LockTask::Revoke => Box::new(
                    least_client
                        .revoke(RevokeRequest::new(lease_id))
                        .map(|_| ())
                        .map_err(|e| eprintln!("revoke failed: {:?}", e)),
                )
                    as Box<dyn Future<Item = (), Error = ()> + Send>,
            })
        });

    tokio::run(task);
}

fn main() {
    let client = Client::builder().add_endpoint("127.0.0.1:2379").build();

    let (mut prepare_lock_send, prepare_lock_recv) = mpsc::channel(0);

    let _leaser_thread = spawn(move || run_leaser(client, prepare_lock_recv));

    for i in 0..5 {
        println!("{:?} {} - acquire mutex to prepare", SystemTime::now(), i);
        let (lock_reply_send, lock_reply_recv) = oneshot::channel();
        let key = (&mut prepare_lock_send)
            .send(LockTask::Lock(lock_reply_send))
            .map_err(|_| oneshot::Canceled)
            .and_then(move |_| lock_reply_recv)
            .wait()
            .unwrap();
        println!(
            "{:?} {} - got mutex to prepare - key = {:x?}",
            SystemTime::now(),
            i,
            key
        );

        // simulate do task ....
        sleep(Duration::from_secs(8));
        // simulate do task ....

        println!(
            "{:?} {} - completed prepare, unlocking mutex",
            SystemTime::now(),
            i
        );
        (&mut prepare_lock_send)
            .send(LockTask::Unlock(key))
            .wait()
            .unwrap();
    }

    prepare_lock_send.send(LockTask::Revoke).wait().unwrap();
}
