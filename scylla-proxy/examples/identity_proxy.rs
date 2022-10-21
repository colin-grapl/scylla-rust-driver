use std::{io::Read as _, net::SocketAddr, str::FromStr};

use scylla_proxy::{Node, Proxy, ShardAwareness};
use tracing::instrument::WithSubscriber;

fn init_logger() {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .without_time()
        .init()
}

fn pause() {
    println!("Press Enter to stop proxy...");
    std::io::stdin().read_exact(&mut [0]).unwrap();
    println!();
}

#[tokio::main]
async fn main() {
    init_logger();

    let node1_real_addr = SocketAddr::from_str("127.0.0.1:9042").unwrap();
    let node1_proxy_addr = SocketAddr::from_str("127.0.0.2:9042").unwrap();
    let proxy = Proxy::builder()
        .with_node(
            Node::builder()
                .real_address(node1_real_addr)
                .proxy_address(node1_proxy_addr)
                .shard_awareness(ShardAwareness::Unaware)
                .build(),
        )
        .build();
    let running_proxy = proxy.run().with_current_subscriber().await.unwrap();

    pause();
    running_proxy.finish().await.unwrap();
}
