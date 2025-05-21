use crate::mempool::MempoolMessage;
use bytes::Bytes;
use crypto::{Digest, PublicKey};
#[cfg(feature = "benchmark")]
use log::info;
use network::ReliableSender;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Instant;

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;

pub struct TransactionProcessor {
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    tx_consensus: Sender<Digest>,
    /// The network addresses of the other mempools.
    mempool_addresses: Vec<(PublicKey, SocketAddr)>,
    /// A network sender to broadcast the batches to the other mempools.
    network: ReliableSender,
}

impl TransactionProcessor {
    pub fn spawn(
        rx_transaction: Receiver<Transaction>,
        tx_consensus: Sender<Digest>,
        mempool_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                rx_transaction,
                tx_consensus,
                mempool_addresses,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.process_transaction(transaction).await;
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current transaction
    async fn process_transaction(&mut self, transaction: Transaction) {
        let digest = Digest::new(&transaction);
        
        #[cfg(feature = "benchmark")]
        {
            if transaction.len() > 8 && transaction[0] == 0u8 {
                if let Ok(id) = transaction[1..9].try_into() {
                    // NOTE: This log entry is used to compute performance.
                    info!(
                        "Transaction {:?} is sample tx {}",
                        digest,
                        u64::from_be_bytes(id)
                    );
                }
            }
            
            // NOTE: This log entry is used to compute performance.
            info!("Transaction {:?} contains {} B", digest, transaction.len());
        }
        
        // Broadcast the transaction through the network.
        let message = MempoolMessage::Transaction(transaction);
        let serialized = bincode::serialize(&message).expect("Failed to serialize transaction");
        
        // Broadcast the transaction through the network
        let (_, addresses): (Vec<_>, _) = self.mempool_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized);
        let _ = self.network.broadcast(addresses, bytes).await;
        
        // Send the transaction through the deliver channel for further processing.
        self.tx_consensus
            .send(digest)
            .await
            .expect("Failed to deliver transaction digest");
    }
}
