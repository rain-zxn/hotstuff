use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, PreImage};
use consensus::{Block, Consensus};
use log::info;
use mempool::Mempool;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use circuit::{ProofService, SecretCircuit};

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Node {
    pub commit: Receiver<Block>,
    pub store: Store,
}

impl Node {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<String>,
    ) -> Result<Self, ConfigError> {
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
        let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);

        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        let pre_image = PreImage::read(key_file)?;
        let secret_encoded = pre_image.secret;
        //let secret = HashOut::from_bytes(&general_purpose::STANDARD.decode(secret_encoded).unwrap());
        let name_encoded = pre_image.name;
        //let name = Digest(HashOut::from_bytes(&general_purpose::STANDARD.decode(name_encoded).unwrap()));

        // build circuit
        let secret_circuit = SecretCircuit::new(secret_encoded.0);

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(&filename)?,
            None => Parameters::default(),
        };

        // Make the data store.
        let store = Store::new(store_path).expect("Failed to create store");

        // Run the proof service.
        let proof_service = ProofService::new(secret_circuit);

        // Make a new mempool.
        Mempool::spawn(
            name_encoded,
            committee.mempool,
            parameters.mempool,
            store.clone(),
            rx_consensus_to_mempool,
            tx_mempool_to_consensus,
        );

        // Run the consensus core.
        Consensus::spawn(
            name_encoded,
            committee.consensus,
            parameters.consensus,
            proof_service,
            store,
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_commit,
        );

        info!("Node {} successfully booted", name_encoded);
        Ok(Self { commit: rx_commit, store: store.clone() })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        PreImage::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        while let Some(_block) = self.commit.recv().await {
            // This is where we can further process committed block.
        }
    }
}
