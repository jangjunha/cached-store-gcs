use async_trait::async_trait;
use cached::IOCachedAsync;
use chrono::{DateTime, Utc};
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::delete::DeleteObjectRequest;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Display;
use std::marker::PhantomData;
use std::time::Duration;

const ENV_BUCKET_KEY: &str = "CACHED_GCS_BUCKET";

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GcsCacheBuildError {
    #[error("gcs client creation error")]
    ClientBuild {
        error: google_cloud_storage::client::google_cloud_auth::error::Error,
    },
    #[error("Bucket name not specified or invalid in env var {env_key:?}: {error:?}")]
    MissingBucket {
        env_key: String,
        error: std::env::VarError,
    },
}

pub struct GcsCache<K, V> {
    pub ttl: Duration,
    pub client: Client,
    pub bucket: String,
    pub prefix: String,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> GcsCache<K, V>
where
    K: Display,
    V: Serialize + DeserializeOwned,
{
    fn generate_key(&self, key: &K) -> String {
        format!("{}{}", self.prefix, key)
    }

    pub async fn new(ttl: Duration, prefix: &str) -> Result<GcsCache<K, V>, GcsCacheBuildError> {
        Ok(GcsCache {
            ttl,
            client: Client::new(
                ClientConfig::default()
                    .with_auth()
                    .await
                    .map_err(|e| GcsCacheBuildError::ClientBuild { error: e })?,
            ),
            bucket: std::env::var(ENV_BUCKET_KEY).map_err(|e| {
                GcsCacheBuildError::MissingBucket {
                    env_key: ENV_BUCKET_KEY.to_string(),
                    error: e,
                }
            })?,
            prefix: prefix.to_string(),
            _phantom: PhantomData,
        })
    }
}

#[derive(Error, Debug)]
pub enum GcsCacheError {
    #[error("gcs error")]
    CloudStorageError(#[from] google_cloud_storage::http::Error),
    #[error("Error deserializing cached value: {cached_value:?}: {error:?}")]
    CacheDeserializationError {
        cached_value: Vec<u8>,
        error: serde_json::Error,
    },
    #[error("Error serializing cached value: {error:?}")]
    CacheSerializationError { error: serde_json::Error },
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CachedGcsValue<V> {
    pub(crate) value: V,
    pub(crate) expires_at: DateTime<Utc>,
}

#[async_trait]
impl<K, V> IOCachedAsync<K, V> for GcsCache<K, V>
where
    K: Display + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
{
    type Error = GcsCacheError;

    async fn cache_get(&self, key: &K) -> Result<Option<V>, Self::Error> {
        let object_key = self.generate_key(key);
        let result = self
            .client
            .download_object(
                &GetObjectRequest {
                    bucket: self.bucket.clone(),
                    object: object_key,
                    ..Default::default()
                },
                &Range::default(),
            )
            .await;
        match result {
            Ok(data) => {
                let val = serde_json::from_slice::<CachedGcsValue<V>>(&data).map_err(|e| {
                    GcsCacheError::CacheDeserializationError {
                        cached_value: data,
                        error: e,
                    }
                })?;
                if Utc::now() <= val.expires_at {
                    Ok(Some(val.value))
                } else {
                    Ok(None)
                }
            }
            Err(google_cloud_storage::http::Error::HttpClient(error))
                if error.status() == Some(http::StatusCode::NOT_FOUND) =>
            {
                Ok(None)
            }
            Err(e) => Err(GcsCacheError::CloudStorageError(e)),
        }
    }

    async fn cache_set(&self, key: K, value: V) -> Result<Option<V>, Self::Error> {
        let object_key = self.generate_key(&key);
        let val = CachedGcsValue {
            value,
            expires_at: Utc::now() + self.ttl,
        };
        let data = serde_json::to_vec(&val)
            .map_err(|e| GcsCacheError::CacheSerializationError { error: e })?;

        let old = self.cache_get(&key).await?;

        self.client
            .upload_object(
                &UploadObjectRequest {
                    bucket: self.bucket.clone(),
                    ..Default::default()
                },
                data,
                &UploadType::Simple(Media::new(object_key)),
            )
            .await
            .map_err(|e| GcsCacheError::CloudStorageError(e))?;

        Ok(old)
    }

    async fn cache_remove(&self, key: &K) -> Result<Option<V>, Self::Error> {
        let object_key = self.generate_key(key);

        let old = self.cache_get(&key).await?;

        self.client
            .delete_object(&DeleteObjectRequest {
                bucket: self.bucket.clone(),
                object: object_key,
                ..Default::default()
            })
            .await
            .map_err(|e| GcsCacheError::CloudStorageError(e))?;

        Ok(old)
    }

    fn cache_set_refresh(&mut self, refresh: bool) -> bool {
        panic!("refresh is not yet supported on GcsCache");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    fn now_millis() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    }

    #[tokio::test]
    async fn test_gcs_cache() {
        let c: GcsCache<u32, u32> = GcsCache::new(
            Duration::from_secs(5),
            &format!("{}:gcs-cache-test/", now_millis()),
        )
        .await
        .unwrap();

        assert!(c.cache_get(&1).await.unwrap().is_none());

        assert!(c.cache_set(1, 100).await.unwrap().is_none());
        assert!(c.cache_get(&1).await.unwrap().is_some());

        sleep(Duration::new(5, 500_000));
        assert!(c.cache_get(&1).await.unwrap().is_none());
    }
}
