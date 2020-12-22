# Azure Bindle Server

This is a storage implementation and server binary using Azure services (namely
Cosmos and blob storage). As the Rust SDK for azure is not currently published,
we did not want to make this part of the main bindle storage implementations
yet. In the future, we may add it back in there, but for now it is only
available as a separate binary.