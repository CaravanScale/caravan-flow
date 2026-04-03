# NiFi Component Analysis

Reference analysis of Apache NiFi's component library, categorized by function.
Source: https://nifi.apache.org/components/ (as of 2026-04-03)

## Processors — Core (Data Transformation & Flow Control)

### Transform
- ReplaceText, ReplaceTextWithMapping, ConvertCharacterSet, ConvertRecord
- JoltTransformJSON, JSLTTransformJSON, TransformXml
- FlattenJson, EvaluateJsonPath, EvaluateXPath, EvaluateXQuery
- ExtractText, ExtractGrok, ExtractHL7Attributes, ExtractEmailHeaders, ExtractEmailAttachments
- AttributesToCSV, AttributesToJSON, UpdateAttribute, FilterAttribute, LookupAttribute
- EncodeContent, CompressContent, ModifyCompression, ModifyBytes

### Record Operations
- QueryRecord, UpdateRecord, RemoveRecordField, RenameRecordField
- PartitionRecord, ForkRecord, SplitRecord, MergeRecord, DeduplicateRecord
- SampleRecord, LookupRecord, ValidateRecord
- ScriptedFilterRecord, ScriptedPartitionRecord, ScriptedTransformRecord, ScriptedValidateRecord
- CalculateRecordStats, ExtractRecordSchema, GenerateRecord

### Route & Control
- RouteOnAttribute, RouteOnContent, RouteText, RouteHL7
- ControlRate, DistributeLoad, EnforceOrder
- DetectDuplicate, MonitorActivity, Wait, Notify, RetryFlowFile

### Split & Merge
- SplitJson, SplitXml, SplitText, SplitAvro, SplitExcel, SplitContent, SplitPCAP
- MergeContent, SegmentContent, PackageFlowFile, UnpackContent

### Enrich
- GeoEnrichIP, GeoEnrichIPRecord, GeohashRecord, ISPEnrichIP
- ForkEnrichment, JoinEnrichment

### Security
- CryptographicHashContent, VerifyContentMAC
- EncryptContentAge, DecryptContentAge, EncryptContentPGP, DecryptContentPGP
- SignContentPGP, VerifyContentPGP

### Validate
- ValidateCsv, ValidateJson, ValidateXml, IdentifyMimeType, ScanContent, ScanAttribute

### Utility
- GenerateFlowFile, DuplicateFlowFile, LogAttribute, LogMessage, DebugFlow
- UpdateCounter, UpdateGauge, CountText, AttributeRollingWindow
- ExecuteProcess, ExecuteStreamCommand, ExecuteScript, ExecuteGroovyScript, InvokeScriptedProcessor

### SQL
- ExecuteSQL, ExecuteSQLRecord, PutSQL
- QueryDatabaseTable, QueryDatabaseTableRecord
- GenerateTableFetch, ListDatabaseTables, UpdateDatabaseTable, PutDatabaseRecord

---

## Processors — Connectors (I/O with External Systems)

### File System
- GetFile, PutFile, FetchFile, ListFile, DeleteFile, TailFile

### FTP/SFTP/SMB
- Get/Put/List/Fetch/Delete for each protocol

### HTTP
- HandleHttpRequest, HandleHttpResponse, ListenHTTP, InvokeHTTP

### TCP/UDP/Syslog
- ListenTCP, PutTCP, ListenUDP, PutUDP
- ListenSyslog, PutSyslog, ParseSyslog, ParseSyslog5424

### WebSocket
- ConnectWebSocket, ListenWebSocket, PutWebSocket

### AWS
- S3: GetS3Object, PutS3Object, ListS3, DeleteS3Object, CopyS3Object, FetchS3Object, GetS3ObjectMetadata, GetS3ObjectTags, TagS3Object
- SQS: GetSQS, PutSQS, DeleteSQS
- SNS: PutSNS
- DynamoDB: GetDynamoDB, PutDynamoDB, PutDynamoDBRecord, DeleteDynamoDB
- Kinesis: ConsumeKinesis, ConsumeKinesisStream, PutKinesisStream, PutKinesisFirehose
- Lambda: PutLambda
- CloudWatch: PutCloudWatchMetric
- AI: StartAwsPollyJob, GetAwsPollyJobStatus, StartAwsTextractJob, GetAwsTextractJobStatus, StartAwsTranscribeJob, GetAwsTranscribeJobStatus, StartAwsTranslateJob, GetAwsTranslateJobStatus

### Azure
- Blob Storage: PutAzureBlobStorage_v12, FetchAzureBlobStorage_v12, ListAzureBlobStorage_v12, DeleteAzureBlobStorage_v12, CopyAzureBlobStorage_v12
- Data Lake: PutAzureDataLakeStorage, FetchAzureDataLakeStorage, ListAzureDataLakeStorage, DeleteAzureDataLakeStorage, MoveAzureDataLakeStorage
- Event Hub: PutAzureEventHub, GetAzureEventHub, ConsumeAzureEventHub
- Queue: PutAzureQueueStorage_v12, GetAzureQueueStorage_v12
- Cosmos DB: PutAzureCosmosDBRecord
- Data Explorer: PutAzureDataExplorer, QueryAzureDataExplorer

### GCP
- Cloud Storage: PutGCSObject, FetchGCSObject, ListGCSBucket, DeleteGCSObject
- Pub/Sub: PublishGCPubSub, ConsumeGCPubSub
- BigQuery: PutBigQuery
- Vision: StartGcpVisionAnnotateFilesOperation, GetGcpVisionAnnotateFilesOperationStatus, StartGcpVisionAnnotateImagesOperation, GetGcpVisionAnnotateImagesOperationStatus

### Messaging
- Kafka: PublishKafka, ConsumeKafka
- AMQP: PublishAMQP, ConsumeAMQP
- JMS: PublishJMS, ConsumeJMS
- MQTT: PublishMQTT, ConsumeMQTT

### Databases
- MongoDB: GetMongo, GetMongoRecord, PutMongo, PutMongoRecord, PutMongoBulkOperations, DeleteMongo, RunMongoAggregation, FetchGridFS, PutGridFS, DeleteGridFS
- Elasticsearch: GetElasticsearch, PutElasticsearchJson, PutElasticsearchRecord, SearchElasticsearch, JsonQueryElasticsearch, PaginatedJsonQueryElasticsearch, DeleteByQueryElasticsearch, UpdateByQueryElasticsearch, ConsumeElasticsearch
- Splunk: GetSplunk, PutSplunk, PutSplunkHTTP, QuerySplunkIndexingStatus
- Redis: PutRedisHashRecord
- Iceberg: PutIcebergRecord

### SaaS
- Box: PutBoxFile, FetchBoxFile, FetchBoxFileInfo, ListBoxFile, ListBoxFileInfo, GetBoxFileCollaborators, GetBoxGroupMembers, ConsumeBoxEnterpriseEvents, ConsumeBoxEvents, CreateBoxFileMetadataInstance, UpdateBoxFileMetadataInstance, DeleteBoxFileMetadataInstance, FetchBoxFileMetadataInstance, ExtractStructuredBoxFileMetadata, ListBoxFileMetadataInstances, CreateBoxMetadataTemplate, ListBoxFileMetadataTemplates, FetchBoxFileRepresentation
- Dropbox: PutDropbox, FetchDropbox, ListDropbox
- Google Drive: PutGoogleDrive, FetchGoogleDrive, ListGoogleDrive
- Salesforce: PutSalesforceObject, QuerySalesforceObject
- Slack: PublishSlack, ConsumeSlack, ListenSlack
- Other: GetHubSpot, GetShopify, GetZendesk, PutZendeskTicket, GetWorkdayReport, QueryAirtableTable, ConsumeTwitter

### Other Protocols
- SNMP: GetSNMP, SetSNMP, SendTrapSNMP, ListenTrapSNMP
- Email: ConsumeIMAP, ConsumePOP3, PutEmail
- HL7: ExtractHL7Attributes, RouteHL7
- OpenTelemetry: ListenOTLP
- Windows: ConsumeWindowsEventLog, ParseEvtx
- NetFlow: ParseNetflowv5

---

## Controller Services

### Record Format (Read/Write)
The most important cross-cutting concern. Processors are format-agnostic — they work on "records" and the reader/writer handles serialization.

**Readers**: AvroReader, CSVReader, ExcelReader, GrokReader, JsonPathReader, JsonTreeReader, ProtobufReader, XMLReader, YamlTreeReader, CEFReader, SyslogReader, Syslog5424Reader, WindowsEventLogReader, ScriptedReader

**Writers**: AvroRecordSetWriter, CSVRecordSetWriter, JsonRecordSetWriter, XMLRecordSetWriter, FreeFormTextRecordSetWriter, ParquetIcebergWriter, ScriptedRecordSetWriter

**Lookups**: ReaderLookup, RecordSetWriterLookup

### Schema Registry
Schema evolution and validation:
- AvroSchemaRegistry, StandardJsonSchemaRegistry, VolatileSchemaCache
- ConfluentSchemaRegistry, ApicurioSchemaRegistry, AmazonGlueSchemaRegistry
- DatabaseTableSchemaRegistry
- AmazonGlueEncodedSchemaReferenceReader, ConfluentEncodedSchemaReferenceReader, ConfluentEncodedSchemaReferenceWriter, ConfluentProtobufMessageNameResolver

### Lookup Services
Enrichment via external data sources:
- CSVRecordLookupService, SimpleCsvFileLookupService
- DatabaseRecordLookupService, SimpleDatabaseLookupService
- ElasticSearchLookupService, ElasticSearchStringLookupService
- MongoDBLookupService
- RestLookupService
- IPLookupService
- PropertiesFileLookupService, XMLFileLookupService
- SimpleKeyValueLookupService
- DistributedMapCacheLookupService
- ScriptedLookupService, SimpleScriptedLookupService

### Record Sinks
Output destinations as shared services:
- DatabaseRecordSink, HttpRecordSink, EmailRecordSink
- SlackRecordSink, UDPEventRecordSink, LoggingRecordSink
- AzureEventHubRecordSink, ZendeskRecordSink
- SiteToSiteReportingRecordSink, ScriptedRecordSink
- RecordSinkServiceLookup

### Connection Pools / Clients
Shared connections across processors:
- DBCPConnectionPool, DBCPConnectionPoolLookup, HikariCPConnectionPool (SQL)
- RedisConnectionPoolService, RedisDistributedMapCacheClientService, SimpleRedisDistributedMapCacheClientService
- ElasticSearchClientServiceImpl
- MongoDBControllerService
- Kafka3ConnectionService, AmazonMSKConnectionService
- JMSConnectionFactoryProvider, JndiJmsConnectionFactoryProvider
- JettyWebSocketClient, JettyWebSocketServer
- SmbjClientProviderService
- DeveloperBoxClientService, JsonConfigBasedBoxClientService

### Caching
Distributed state for dedup, enrichment, counters:
- MapCacheClientService, MapCacheServer
- SetCacheClientService, SetCacheServer
- EmbeddedHazelcastCacheManager, ExternalHazelcastCacheManager, HazelcastMapCacheClient

### Credentials / Auth
Shared authentication across processors:
- AWSCredentialsProviderControllerService
- GCPCredentialsControllerService
- StandardAzureCredentialsControllerService, ADLSCredentialsControllerService, ADLSCredentialsControllerServiceLookup, AzureStorageCredentialsControllerService_v12, AzureStorageCredentialsControllerServiceLookup_v12
- StandardOauth2AccessTokenProvider, JWTBearerOAuth2AccessTokenProvider
- StandardDropboxCredentialService
- KerberosKeytabUserService, KerberosPasswordUserService, KerberosTicketCacheUserService
- AwsRdsIamDatabasePasswordProvider

### Security
Keys, SSL, encryption services:
- StandardSSLContextService, StandardRestrictedSSLContextService, PEMEncodedSSLContextProvider
- StandardPGPPrivateKeyService, StandardPGPPublicKeyService, StandardPrivateKeyService
- StandardS3EncryptionService

### Network / HTTP
- StandardProxyConfigurationService
- StandardWebClientServiceProvider
- StandardHttpContextMap

### File Resources
- StandardFileResourceService, AzureBlobStorageFileResourceService
- AzureDataLakeStorageFileResourceService, GCSFileResourceService, S3FileResourceService

### Iceberg / Data Lake
- ADLSIcebergFileIOProvider, S3IcebergFileIOProvider, RESTIcebergCatalog

### Database
- StandardDatabaseDialectService
- StandardKustoIngestService, StandardKustoQueryService

---

## Reporting Tasks

### System Health
- MonitorDiskUsage — monitors and reports disk usage
- MonitorMemory — monitors and reports memory usage
- ControllerStatusReportingTask — reports controller status

### Provenance / Audit
- SiteToSiteProvenanceReportingTask — provenance data via Site-to-Site
- AzureLogAnalyticsProvenanceReportingTask — provenance to Azure Log Analytics

### Metrics
- SiteToSiteMetricsReportingTask — metrics via Site-to-Site
- SiteToSiteStatusReportingTask — status via Site-to-Site
- SiteToSiteBulletinReportingTask — bulletins via Site-to-Site
- AzureLogAnalyticsReportingTask — metrics to Azure Log Analytics

### Custom
- ScriptedReportingTask — custom reporting via scripts

---

## Parameter Providers

Secrets management — where config values come from at runtime:

- EnvironmentVariableParameterProvider — environment variables
- DatabaseParameterProvider — database tables
- AwsSecretsManagerParameterProvider — AWS Secrets Manager
- AzureKeyVaultSecretsParameterProvider — Azure Key Vault
- GcpSecretManagerParameterProvider — GCP Secret Manager
- HashiCorpVaultParameterProvider — HashiCorp Vault
- KubernetesSecretParameterProvider — Kubernetes Secrets
- OnePasswordParameterProvider — 1Password

---

## Flow Registry Clients

Version control for flow definitions:
- GitHubFlowRegistryClient
- GitLabFlowRegistryClient
- BitbucketFlowRegistryClient
- AzureDevOpsFlowRegistryClient
- NifiRegistryFlowRegistryClient

---

## Flow Analysis Rules

Governance and compliance — enforcing policies on flow design:
- DisallowComponentType — prevents use of specified component types
- RequireServerSSLContextService — enforces SSL for servers
- RestrictBackpressureSettings — restricts backpressure config
- RestrictFlowFileExpiration — restricts expiration settings

---

## What Matters for zinc-flow

### Priority 1 — Core cross-cutting services
1. **Record Format (Read/Write)** — NiFi's power comes from format-agnostic processors. A RecordReader/RecordWriter interface lets one processor handle JSON, CSV, Avro without knowing the format.
2. **Lookup Services** — enrichment is a core pattern ("look up this IP's geo" or "look up this user ID in the database").
3. **Connection Pools** — shared database/Redis/cache connections across processors.

### Priority 2 — Operational
4. **Credentials** — shared auth so multiple processors don't each need their own config.
5. **Caching** — distributed map/set cache for dedup, enrichment, state.
6. **Schema Registry** — schema evolution for record-based processing.

### Priority 3 — Later phases
7. **Reporting Tasks** — system health, metrics, provenance.
8. **Parameter Providers** — secrets management integration.
9. **Flow Registry** — version control for flow definitions.
10. **Flow Analysis Rules** — governance/compliance.

### Key Insight
NiFi is ~70% connectors, ~30% core logic. The connectors are thin wrappers around SDKs. The real value is:
- **Record abstraction** — format-agnostic processing
- **Routing engine** — attribute-based routing (zinc-flow already has this)
- **Flow control** — backpressure, rate limiting, ordering
- **Shared services** — connection pools, credentials, caching, schemas
