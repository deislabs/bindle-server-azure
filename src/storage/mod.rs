use std::convert::TryInto;
use std::sync::Arc;
use std::{collections::HashSet, io::Write};

use azure_core::errors::{AzureError, UnexpectedHTTPResult};
use azure_core::*;
use azure_cosmos::clients::{CollectionClient, CosmosClient};
use azure_cosmos::prelude::*;
use azure_cosmos::responses::{GetDocumentResponse, QueryDocumentsResponse};
use azure_storage::blob::prelude::*;
use azure_storage::client;
use azure_storage::key_client::KeyClient;
use bindle::storage::{Result, Storage, StorageError};
use bindle::Id;
use log::{debug, error, trace};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncRead;
use tokio::stream::StreamExt;

const LABEL_COLLECTION: &str = "labels";
const INVOICE_COLLECTION: &str = "invoices";
const BLOB_CONTAINER: &str = "parcels";
// TODO: Switch to this once parameterized queries works properly in the SDK
const MISSING_PARCEL_QUERY: &str = "SELECT l.label.sha256 from l where l.label.sha256 IN @sha_list";

/// An internal representation of an invoice. We have to have an `id` field for
/// cosmos to accept it, so we use the ID sha for it
#[derive(Serialize, Deserialize, Debug)]
struct InternalInvoice {
    id: String,
    invoice: bindle::Invoice,
}

/// An internal representation of an Label. We have to have an `id` field for
/// cosmos to accept it, so we use the ID sha for it
#[derive(Serialize, Deserialize, Debug)]
struct InternalLabel {
    id: String,
    label: bindle::Label,
}

/// The form we expect a label query to come back from Cosmos with
#[derive(Deserialize)]
struct LabelQueryResponse {
    sha256: String,
}

/// A Bindle [`Storage`](bindle::storage::Storage) implementation for Azure.
/// This implementation uses Cosmos DB to store invoices and labels and Azure
/// blob storage for storing parcel data.
#[derive(Clone)]
pub struct AzureStorage {
    invoices_client: CollectionClient,
    labels_client: CollectionClient,
    blob_client: KeyClient,
}

/// The necessary configuration info for connecting to Cosmos. The given
/// authorization token should be a primary token (and not a resource token)
/// with full permissions on the `labels` and `invoices` collections. Please
/// note that the given database must already have the `labels` and `invoices`
/// collections created
pub struct CosmosConfig {
    /// Cosmos account name
    pub account_name: String,
    /// The resource token for authenticating with Cosmos
    pub authorization_token: String,
    /// The name of the database to use
    pub database_name: String,
}

/// The necessary configuration for connecting to Azure Blob storage. The given
/// storage account must have a container called `parcels` already created, and
/// the given SAS token must have access to create, read, and delete blobs
pub struct AzureBlobConfig {
    /// The storage account name
    pub account_name: String,
    /// An account SAS token for authentication
    pub sas_token: String,
}

impl AzureStorage {
    pub fn new(cosmos: CosmosConfig, blob: AzureBlobConfig) -> anyhow::Result<Self> {
        // TODO: Is there any other type of config we want to do with the client?
        let http_client: Arc<Box<dyn HttpClient>> = Arc::new(Box::new(reqwest::Client::new()));
        let cosmos_client = CosmosClient::new(
            http_client,
            cosmos.account_name,
            AuthorizationToken::primary_from_base64(&cosmos.authorization_token)?,
        );
        let cosmos_client = cosmos_client.into_database_client(cosmos.database_name);

        Ok(AzureStorage {
            invoices_client: cosmos_client
                .clone()
                .into_collection_client(INVOICE_COLLECTION),
            labels_client: cosmos_client.into_collection_client(LABEL_COLLECTION),
            blob_client: client::with_azure_sas(&blob.account_name, &blob.sas_token),
        })
    }
}

#[async_trait::async_trait]
impl Storage for AzureStorage {
    async fn create_invoice(&self, inv: &bindle::Invoice) -> Result<Vec<bindle::Label>> {
        // It is illegal to create a yanked invoice.
        if inv.yanked.unwrap_or(false) {
            return Err(StorageError::CreateYanked);
        }

        debug!(
            "Storing invoice {} in Cosmos collection {}",
            inv.bindle.id, INVOICE_COLLECTION
        );
        if let Err(e) = self
            .invoices_client
            .create_document()
            .with_partition_keys(&(inv.bindle.id.name()).into())
            .execute_with_document(&InternalInvoice {
                id: inv.canonical_name(),
                invoice: inv.clone(),
            })
            .await
        {
            return Err(handle_cosmos_error(e));
        }

        trace!(
            "Checking for missing parcels listed in newly created invoice {}",
            inv.bindle.id
        );

        // Note: this will not allocate
        let zero_vec = Vec::with_capacity(0);
        // Get a list of all shas
        let sha_list: Vec<String> = inv
            .parcel
            .as_ref()
            .unwrap_or(&zero_vec)
            .iter()
            .map(|k| k.label.sha256.clone())
            .collect();

        // HACK: Just doing string formatting right now to get the right query.
        // This is obviously dangerous, but will be fixed soon
        let query = format!(
            "SELECT l.label.sha256 from l where l.label.sha256 IN {}",
            // Arrays in Cosmos SQL need to be parens
            serde_json::to_string(&sha_list)
                .unwrap()
                .replace("[", "(")
                .replace("]", ")")
        );
        trace!("Using query: {}", query);
        let resp: QueryDocumentsResponse<LabelQueryResponse> = self
            .labels_client
            .query_documents()
            .with_query(&Query::new(&query))
            .with_query_cross_partition(true)
            .with_parallelize_cross_partition_query(true)
            .execute()
            .await
            .map_err(handle_cosmos_error)?;
        let found: HashSet<String> = resp
            .into_raw()
            .results
            .into_iter()
            .map(|r| r.sha256)
            .collect();

        Ok(inv
            .parcel
            .as_ref()
            .unwrap_or(&zero_vec)
            .iter()
            .filter(|p| !found.contains(&p.label.sha256))
            .cloned()
            .map(|p| p.label)
            .collect())
    }

    async fn get_yanked_invoice<I>(&self, id: I) -> Result<bindle::Invoice>
    where
        I: TryInto<Id> + Send,
        I::Error: Into<StorageError>,
    {
        let parsed_id = id.try_into().map_err(|e| e.into())?;

        let sha = parsed_id.sha();

        // TODO: DOCUMENT EXPECTED PARTITION KEYS
        trace!("Fetching invoice {} from Cosmos", parsed_id);
        let inv = match self
            .invoices_client
            .clone()
            .into_document_client(sha, (parsed_id.name()).into())
            .get_document()
            .execute::<InternalInvoice>()
            .await
        {
            Ok(GetDocumentResponse::Found(doc)) => doc.document.document,
            Ok(GetDocumentResponse::NotFound(_)) => {
                return Err(StorageError::NotFound);
            }
            Err(e) => return Err(handle_cosmos_error(e)),
        };

        Ok(inv.invoice)
    }

    async fn yank_invoice<I>(&self, id: I) -> Result<()>
    where
        I: TryInto<Id> + Send,
        I::Error: Into<StorageError>,
    {
        // Get the existing invoice
        let mut inv = self.get_yanked_invoice(id).await?;

        inv.yanked = Some(true);

        self.invoices_client
            .replace_document()
            .with_document_id(&inv.canonical_name())
            .with_partition_keys(&(inv.bindle.id.name()).into())
            .execute_with_document(&InternalInvoice {
                id: inv.canonical_name(),
                invoice: inv,
            })
            .await
            .map_err(handle_cosmos_error)?;
        Ok(())
    }

    async fn create_parcel<R: AsyncRead + Unpin + Send + Sync>(
        &self,
        label: &bindle::Label,
        data: &mut R,
    ) -> Result<()> {
        // First we are going to create the append blob so we can send data in chunks (as we receive them)
        self.blob_client
            .put_append_blob()
            .with_container_name(BLOB_CONTAINER)
            .with_blob_name(&label.sha256)
            .with_content_type(&label.media_type)
            .finalize()
            .await
            .map_err(handle_azure_error)?;

        // Now we are going to loop over the reader until we hit EOF (no bytes read)
        let mut stream = tokio::io::reader_stream(data);
        let mut hasher = Sha256::new();
        while let Some(res) = stream.next().await {
            let raw = res?;
            println!("Got data {}", String::from_utf8_lossy(&raw));
            // In here, we append a block of data and write the data into the hasher
            hasher.write_all(&raw)?;
            self.blob_client
                .put_append_block()
                .with_container_name(BLOB_CONTAINER)
                .with_blob_name(&label.sha256)
                .with_body(&raw)
                .finalize()
                .await
                .map_err(handle_azure_error)?;
            // TODO: would it be better to just reallocate a new vec each loop?
        }

        // Now validate the hash of the data
        let hash = hasher.finalize();
        if format!("{:x}", hash) != label.sha256 {
            debug!("Got invalid hash {:x}. Expected {}", hash, label.sha256);
            // Clean up the blob
            if let Err(e) = self
                .blob_client
                .delete_blob()
                .with_container_name(BLOB_CONTAINER)
                .with_blob_name(&label.sha256)
                .with_delete_snapshots_method(DeleteSnapshotsMethod::Include)
                .finalize()
                .await
            {
                error!(
                    "Unable to clean up blob {} after digest mismatch: {:?}",
                    label.sha256, e
                );
            }
            return Err(StorageError::DigestMismatch);
        }

        debug!(
            "Storing label {} in Cosmos collection {}",
            label.sha256, LABEL_COLLECTION
        );
        if let Err(e) = self
            .labels_client
            .create_document()
            .with_partition_keys(&(&label.sha256).into())
            .execute_with_document(&InternalLabel {
                id: label.sha256.clone(),
                label: label.clone(),
            })
            .await
        {
            error!("Unable to create label in storage: {:?}", e);
            // TODO: How do we want to handle the already created blob here?
            return Err(handle_cosmos_error(e));
        }
        Ok(())
    }

    async fn get_parcel(
        &self,
        parcel_id: &str,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send + Sync>> {
        // HACK: None of the streaming options I have tried have worked, so
        // right now we read the whole thing into memory. See the commented out
        // code at the end of this file

        let resp = self
            .blob_client
            .get_blob()
            .with_container_name(BLOB_CONTAINER)
            .with_blob_name(parcel_id)
            .finalize()
            .await
            .map_err(handle_azure_error)?;

        Ok(Box::new(std::io::Cursor::new(resp.data)))
    }

    async fn get_label(&self, parcel_id: &str) -> Result<bindle::Label> {
        // TODO: DOCUMENT EXPECTED PARTITION KEYS
        trace!("Fetching label {} from Cosmos", parcel_id);
        let label = match self
            .labels_client
            .clone()
            .into_document_client(parcel_id.to_owned(), (&parcel_id.to_owned()).into())
            .get_document()
            .execute::<InternalLabel>()
            .await
        {
            Ok(GetDocumentResponse::Found(doc)) => doc.document.document,
            Ok(GetDocumentResponse::NotFound(_)) => {
                return Err(StorageError::NotFound);
            }
            Err(e) => return Err(handle_cosmos_error(e)),
        };

        Ok(label.label)
    }
}

fn handle_cosmos_error(e: azure_cosmos::CosmosError) -> StorageError {
    // Attempt to get a concrete error type out, otherwise return a generic error
    let original = e.to_string();
    if let Ok(e) = e.downcast::<UnexpectedHTTPResult>() {
        match e.status_code() {
            // Mappings from https://docs.microsoft.com/en-us/rest/api/cosmos-db/create-a-document
            StatusCode::FORBIDDEN => {
                error!("Reached storage limit on cosmos partition: {:#?}", e);
                StorageError::Other("Storage limit reached".to_string())
            }
            StatusCode::CONFLICT => StorageError::Exists,
            StatusCode::BAD_REQUEST => StorageError::Other("Body is invalid".to_string()),
            StatusCode::PAYLOAD_TOO_LARGE => {
                StorageError::Other("Document is too large".to_string())
            }
            _ => {
                error!(
                    "Received an unexpected status code {}: {}",
                    e.status_code(),
                    original
                );
                StorageError::Other(e.to_string())
            }
        }
    } else {
        error!("Got unknown error from Cosmos: {:#?}", original);
        StorageError::Other(original)
    }
}

fn handle_azure_error(e: AzureError) -> StorageError {
    match e {
        AzureError::IOError(e) => StorageError::Io(e),
        _ => StorageError::Other(e.to_string()),
    }
}

// Right now both the built in stream option in the Rust SDK and this attempt
// below were trying to stream back chunks, but something in the Rust SDK call
// chain is not `Unpin` and so we can't return it

// A custom implementor of AsyncRead to avoid a bunch of stream issues and
// missing `Sync` by using the stream from the Rust SDK
// struct ParcelReader {
//     client: KeyClient,
//     current: u64,
//     end: u64,
//     chunk_size: u64,
//     parcel_id: String,
//     current_range: Option<Range>,
// }

// impl ParcelReader {
//     async fn new(client: KeyClient, chunk_size: u64, parcel_id: String) -> Result<ParcelReader> {
//         // I really don't like having to fetch this, but in order to stream the
//         // data in chunks, we need to know the final size. The possible solution
//         // here is to use a cache. Another is that on the first request, we can get the blob size from the returned data
//         let data = client
//             .get_blob_properties()
//             .with_container_name(BLOB_CONTAINER)
//             .with_blob_name(&parcel_id)
//             .finalize()
//             .await
//             .map_err(handle_azure_error)?;
//         Ok(ParcelReader {
//             client,
//             current: 0,
//             end: data.blob.content_length,
//             chunk_size,
//             parcel_id,
//             current_range: None,
//         })
//     }
// }

// impl AsyncRead for ParcelReader {
//     fn poll_read(
//         self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//         buf: &mut [u8],
//     ) -> Poll<std::io::Result<usize>> {
//         // If we are waiting for another read, use the current range, otherwise,
//         // generate the next range of bytes to expect
//         let range = match self.current_range.take() {
//             Some(r) => r,
//             None => {
//                 if self.current + self.chunk_size > self.end {
//                     Range::new(self.current, self.end)
//                 } else {
//                     Range::new(self.current, self.current + self.chunk_size)
//                 }
//             }
//         };

//         // TODO: We might be able to take a reference to the Get request, but
//         // right now it has a bunch of params and lifetimes I don't want to
//         // worry about
//         let mut fut = self
//             .client
//             .get_blob()
//             .with_container_name(BLOB_CONTAINER)
//             .with_blob_name(&self.parcel_id)
//             .with_range(&range)
//             .finalize();

//         match Pin::new(&mut fut).poll(cx) {
//             Poll::Pending => {
//                 // put the range back
//                 self.current_range = Some(range);
//                 return Poll::Pending;
//             }
//             Poll::Ready(res) => {
//                 match res {
//                     Ok(resp) => {
//                         let written = match buf.write(&resp.data) {
//                             Err(e) => return Poll::Ready(Err(e)),
//                             Ok(s) => s,
//                         };
//                         // We got and read data successfully, now update internal state
//                         self.current = self.current + self.chunk_size;
//                         return Poll::Ready(Ok(written));
//                     }
//                     Err(e) => {
//                         return Poll::Ready(Err(std::io::Error::new(
//                             std::io::ErrorKind::Other,
//                             e.to_string(),
//                         )))
//                     }
//                 }
//             }
//         }
//     }
// }
