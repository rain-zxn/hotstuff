use crate::config::{Committee, Stake};
use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{Block, QC, TC};
use bytes::Bytes;
use crypto::{Digest, PublicKey, SignatureService};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info};
use network::{CancelHandler, ReliableSender};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
pub enum ProposerMessage {
    Make(Round, QC, Option<TC>),
    Cleanup(Vec<Digest>),
}

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    rx_mempool: Receiver<Digest>,
    rx_message: Receiver<ProposerMessage>,
    tx_loopback: Sender<Block>,
    // Key: Transaction digest
    // Value: Set of transactions that can follow this one (their first 16 bytes match this tx's last 16 bytes)
    buffer: HashMap<Digest, HashSet<Digest>>,
    // The latest block's digest that was produced
    last_block_digest: Option<Digest>,
    network: ReliableSender,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        rx_mempool: Receiver<Digest>,
        rx_message: Receiver<ProposerMessage>,
        tx_loopback: Sender<Block>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                rx_mempool,
                rx_message,
                tx_loopback,
                buffer: HashMap::new(),
                last_block_digest: None,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    async fn make_block(&mut self, round: Round, qc: QC, tc: Option<TC>) {
        // Find the next transaction to include in the block
        let payload = self.get_next_transaction();
        
        // Generate a new block.
        let block = Block::new(
            qc,
            tc,
            self.name,
            round,
            /* payload */ vec![payload],
            self.signature_service.clone(),
        )
        .await;

        if !block.payload.is_empty() {
            info!("Created {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                // NOTE: This log entry is used to compute performance.
                info!("Created {} -> {:?}", block, x);
            }
        }
        debug!("Created {:?}", block);

        // Broadcast our new block.
        debug!("Broadcasting {:?}", block);
        let (names, addresses): (Vec<_>, _) = self
            .committee
            .broadcast_addresses(&self.name)
            .iter()
            .cloned()
            .unzip();
        let message = bincode::serialize(&ConsensusMessage::Propose(block.clone()))
            .expect("Failed to serialize block");
        let handles = self
            .network
            .broadcast(addresses, Bytes::from(message))
            .await;

        // Send our block to the core for processing.
        self.tx_loopback
            .send(block)
            .await
            .expect("Failed to send block");

        // Control system: Wait for 2f+1 nodes to acknowledge our block before continuing.
        let mut wait_for_quorum: FuturesUnordered<_> = names
            .into_iter()
            .zip(handles.into_iter())
            .map(|(name, handler)| {
                let stake = self.committee.stake(&name);
                Self::waiter(handler, stake)
            })
            .collect();

        let mut total_stake = self.committee.stake(&self.name);
        while let Some(stake) = wait_for_quorum.next().await {
            total_stake += stake;
            if total_stake >= self.committee.quorum_threshold() {
                break;
            }
        }
    }

    // Get the next transaction to include in a block based on chain rules
    fn get_next_transaction(&mut self) -> Digest {
        // If there are no transactions or we have no last block, return an empty transaction
        if self.buffer.is_empty() || self.last_block_digest.is_none() {
            return Digest([0; 32]); // Default empty transaction
        }
        
        let last_digest = self.last_block_digest.unwrap();
        
        // Extract the last 16 bytes of the last block's digest
        let last_16_bytes = &last_digest.0[16..32];
        
        // Look for a transaction whose first 16 bytes match the last 16 bytes of the last block
        for (tx_digest, _) in &self.buffer {
            // Check if first 16 bytes of this transaction match the last 16 bytes of previous block
            if &tx_digest.0[0..16] == last_16_bytes {
                self.last_block_digest = Some(*tx_digest);
                return *tx_digest;
            }
        }
        
        // If no matching transaction is found, return an empty transaction
        Digest([0; 32])
    }
    
    // Add transaction to buffer with proper linking
    fn add_transaction(&mut self, digest: Digest) {
        // Extract the first and last 16 bytes of the transaction
        let first_16_bytes = &digest.0[0..16];
        let last_16_bytes = &digest.0[16..32];
        
        // Find all transactions whose last 16 bytes match the first 16 bytes of this transaction
        for (tx_digest, followers) in &mut self.buffer {
            if &tx_digest.0[16..32] == first_16_bytes {
                // This transaction can be followed by our new transaction
                followers.insert(digest);
            }
        }
        
        // Add this transaction to the buffer with an empty set of followers
        if !self.buffer.contains_key(&digest) {
            self.buffer.insert(digest, HashSet::new());
        }
        
        // Find all transactions whose first 16 bytes match the last 16 bytes of this transaction
        // and add them as followers
        let mut followers = HashSet::new();
        for tx_digest in self.buffer.keys() {
            if &tx_digest.0[0..16] == last_16_bytes {
                followers.insert(*tx_digest);
            }
        }
        
        // Update the followers set for this transaction
        if !followers.is_empty() {
            self.buffer.insert(digest, followers);
        }
    }
    
    // Clean up confirmed transactions and their related transactions
    fn cleanup_transactions(&mut self, digests: &[Digest]) {
        for digest in digests {
            if let Some(followers) = self.buffer.remove(digest) {
                // Also remove all transactions that could follow this one
                for follower in followers {
                    self.buffer.remove(&follower);
                }
            }
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(digest) = self.rx_mempool.recv() => {
                    self.add_transaction(digest);
                },
                Some(message) = self.rx_message.recv() => match message {
                    ProposerMessage::Make(round, qc, tc) => self.make_block(round, qc, tc).await,
                    ProposerMessage::Cleanup(digests) => {
                        self.cleanup_transactions(&digests);
                    }
                }
            }
        }
    }
}
