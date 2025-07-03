use std::convert::Infallible;
use warp::{Filter, Reply};
use store::Store;
use serde_json::json;
use plonky2::field::goldilocks_field::GoldilocksField;
use plonky2::plonk::proof::Proof;
use plonky2_poseidon::poseidon::PoseidonGoldilocksConfig;
use base64::{Engine as _, engine::general_purpose};

pub struct RpcService {
    store: Store,
}

impl RpcService {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub async fn start(&self, port: u16) {
        let store = self.store.clone();
        
        let get_proof = warp::path!("api" / "proof" / String)
            .and(warp::get())
            .and_then(move |prev_str: String| {
                let store = store.clone();
                async move {
                    // Parse prev as hex-encoded 32 bytes, convert to [GoldilocksField; 4]
                    let prev_key = match hex::decode(&prev_str) {
                        Ok(bytes) if bytes.len() == 32 => {
                            // Convert 32 bytes to [GoldilocksField; 4] (8 bytes per field)
                            let mut prev_array = [GoldilocksField::ZERO; 4];
                            for i in 0..4 {
                                let field_bytes = &bytes[i*8..(i+1)*8];
                                let field_u64 = u64::from_le_bytes(field_bytes.try_into().unwrap());
                                prev_array[i] = GoldilocksField::from_canonical_u64(field_u64);
                            }
                            match bincode::serialize(&prev_array) {
                                Ok(key) => key,
                                Err(_) => return Ok(warp::reply::with_status(
                                    warp::reply::json(&json!({"error": "Failed to serialize prev array"})),
                                    warp::http::StatusCode::INTERNAL_SERVER_ERROR
                                ))
                            }
                        }
                        Ok(_) => return Ok(warp::reply::with_status(
                            warp::reply::json(&json!({"error": "Prev must be exactly 32 hex bytes"})),
                            warp::http::StatusCode::BAD_REQUEST
                        )),
                        Err(_) => return Ok(warp::reply::with_status(
                            warp::reply::json(&json!({"error": "Invalid hex format"})),
                            warp::http::StatusCode::BAD_REQUEST
                        ))
                    };

                    let mut store_copy = store.clone();
                    match store_copy.read(prev_key).await {
                        Ok(Some(proof_bytes)) => {
                            match bincode::deserialize::<Proof<GoldilocksField, PoseidonGoldilocksConfig, 2>>(&proof_bytes) {
                                Ok(proof) => {
                                    let proof_serialized = bincode::serialize(&proof).unwrap();
                                    let proof_hex = hex::encode(&proof_serialized);
                                    Ok(warp::reply::with_status(
                                        warp::reply::json(&json!({
                                            "prev": prev_str,
                                            "proof": proof_hex
                                        })),
                                        warp::http::StatusCode::OK
                                    ))
                                }
                                Err(_) => Ok(warp::reply::with_status(
                                    warp::reply::json(&json!({"error": "Failed to deserialize proof"})),
                                    warp::http::StatusCode::INTERNAL_SERVER_ERROR
                                ))
                            }
                        }
                        Ok(None) => Ok(warp::reply::with_status(
                            warp::reply::json(&json!({"error": "Proof not found"})),
                            warp::http::StatusCode::NOT_FOUND
                        )),
                        Err(_) => Ok(warp::reply::with_status(
                            warp::reply::json(&json!({"error": "Database error"})),
                            warp::http::StatusCode::INTERNAL_SERVER_ERROR
                        ))
                    }
                }
            });

        let cors = warp::cors()
            .allow_any_origin()
            .allow_headers(vec!["content-type"])
            .allow_methods(vec!["GET", "POST", "OPTIONS"]);

        let routes = get_proof.with(cors);

        warp::serve(routes)
            .run(([0, 0, 0, 0], port))
            .await;
    }
}
