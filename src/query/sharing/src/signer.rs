// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::time::Duration;

use anyhow::anyhow;
use anyhow::Result;
use bytes::Bytes;
use http::header::AUTHORIZATION;
use http::header::CONTENT_LENGTH;
use http::Method;
use http::Request;
use moka::sync::Cache;
use opendal::http_util::AsyncBody;
use opendal::http_util::HttpClient;
use opendal::ops::Operation;
use opendal::ops::PresignedRequest;

/// SharedSigner is used to track presign request, and it's response.
///
/// There is an internal cache about presign request. Getting an expired
/// request will get `None`. Please sign it again.
#[derive(Clone)]
pub struct SharedSigner {
    endpoint: String,
    token: String,
    cache: Cache<PresignRequest, PresignedRequest>,
    client: HttpClient,
}

impl Debug for SharedSigner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSigner")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl SharedSigner {
    /// Create a new SharedSigner.
    pub fn new(endpoint: &str, token: &str) -> Self {
        let cache = Cache::builder()
            // Databend Cloud Presign will expire after 3600s (1 hour).
            // We will expire them 10 minutes before to avoid edge cases.
            .time_to_live(Duration::from_secs(3000))
            .build();

        Self {
            endpoint: endpoint.to_string(),
            token: token.to_string(),
            cache,
            client: HttpClient::new(),
        }
    }

    /// Get a presign request.
    pub fn get(&self, path: &str, op: Operation) -> Option<PresignedRequest> {
        self.cache.get(&PresignRequest {
            path: path.to_string(),
            op,
        })
    }

    /// Fetch a presigned request. If not found, build a new one by sign.
    pub async fn fetch(&self, path: &str, op: Operation) -> Result<PresignedRequest> {
        match self.get(path, op) {
            Some(v) => Ok(v),
            None => {
                self.sign(PresignRequest::new(path, op)).await?;
                self.get(path, op)
                    .ok_or_else(|| anyhow!("presigned request should exist, but not"))
            }
        }
    }

    /// Set a presigned request.
    ///
    /// This operation will update the expiry time about this request.
    pub fn set(&self, path: &str, op: Operation, signed: PresignedRequest) {
        self.cache.insert(PresignRequest::new(path, op), signed)
    }

    /// Sign a reqeust.
    pub async fn sign(&self, req: PresignRequest) -> Result<()> {
        self.sign_inner(vec![req]).await
    }

    /// Batch sign multiple requests at once.
    pub async fn batch_sign(&self, reqs: Vec<PresignRequest>) -> Result<()> {
        self.sign_inner(reqs).await
    }

    /// Databend Cloud Presign API will be:
    ///
    /// ```shell
    /// curl -X POST \
    ///      https://endpoint/tenant/<tenant_id>/database/<db_id>/table/<table_id>/presign \
    ///      --header 'Authorization Bearer OIDC TOKEN' \
    ///      --data-raw '[{"path": "file_a", "method": "GET"}, {"path": "file_b", "method": "PUT"}'
    ///
    /// [
    ///     {
    ///        "path": "file_a",
    ///        "method": "GET",
    ///        "url": "https://exmaple.com",
    ///        "headers": {
    ///           "host": "example.com"
    ///        },
    ///        "expires_in": "Sun, 06 Nov 1994 08:49:37 GMT"
    ///    },
    ///     {
    ///        "path": "file_b",
    ///        "method": "PUT",
    ///        "url": "https://exmaple.com",
    ///        "headers": {
    ///           "host": "example.com"
    ///        },
    ///        "expires_in": "Sun, 06 Nov 1994 08:49:37 GMT"
    ///     }
    /// ]
    /// ```
    async fn sign_inner(&self, reqs: Vec<PresignRequest>) -> Result<()> {
        let reqs: Vec<PresignRequestItem> = reqs
            .into_iter()
            .map(|v| PresignRequestItem {
                path: v.path,
                method: to_method(v.op),
            })
            .collect();
        let bs = Bytes::from(serde_json::to_vec(&reqs)?);

        let req = Request::builder()
            .method(Method::POST)
            .uri(&self.endpoint)
            .header(AUTHORIZATION, format!("Bearer {}", self.token))
            .header(CONTENT_LENGTH, bs.len())
            .body(AsyncBody::Bytes(bs))?;
        let resp = self.client.send_async(req).await?;
        let bs = resp.into_body().bytes().await?;
        let items: Vec<PresignResponseItem> = serde_json::from_slice(&bs)?;

        for item in items {
            self.cache.insert(
                PresignRequest::new(&item.path, from_method(&item.method)),
                item.into(),
            );
        }

        Ok(())
    }
}

/// PresignRequest struct represent a request to be signed.
#[derive(Hash, Eq, PartialEq)]
pub struct PresignRequest {
    path: String,
    op: Operation,
}

impl PresignRequest {
    /// Create a new PresignRequest.
    pub fn new(path: &str, op: Operation) -> Self {
        Self {
            path: path.to_string(),
            op,
        }
    }
}

fn to_method(op: Operation) -> String {
    match op {
        Operation::Read => "GET".to_string(),
        v => unimplemented!("not supported operation: {v}"),
    }
}

fn from_method(method: &str) -> Operation {
    match method {
        "GET" => Operation::Read,
        v => unimplemented!("not supported operation: {v}"),
    }
}

#[derive(serde::Serialize)]
struct PresignRequestItem {
    method: String,
    path: String,
}

#[derive(serde::Deserialize)]
struct PresignResponseItem {
    method: String,
    path: String,
    url: String,
    headers: HashMap<String, String>,
    /// Keep this for future using.
    #[allow(unused)]
    expires_in: String,
}

impl From<PresignResponseItem> for PresignedRequest {
    fn from(v: PresignResponseItem) -> Self {
        PresignedRequest::new(
            v.method.parse().expect("must be valid method"),
            v.url.parse().expect("must be valid uri"),
            v.headers
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.parse().expect("header name must be valid"),
                        v.parse().expect("header value must be valid"),
                    )
                })
                .collect(),
        )
    }
}