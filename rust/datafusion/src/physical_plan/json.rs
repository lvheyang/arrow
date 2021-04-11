// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Execution plan for reading CSV files

use std::any::Any;
use std::fs::File;
use std::io::BufReader;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::Stream;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::json;
use arrow::record_batch::RecordBatch;

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{common, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream};

/// Json file read option
#[derive(Copy, Clone)]
pub struct JsonReadOptions<'a> {
    /// An optional schema representing the CSV files. If None, Json reader will try to infer it
    /// based on data in file.
    pub schema: Option<&'a Schema>,
    /// Max number of rows to read from Json files for schema inference if needed. Defaults to 1000.
    pub schema_infer_max_records: usize,
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".json".
    pub file_extension: &'a str,
}

impl<'a> JsonReadOptions<'a> {
    /// Create a new Json read option with default presets
    pub fn new() -> Self {
        Self {
            schema: None,
            schema_infer_max_records: 1000,
            file_extension: ",json",
        }
    }

    /// Specify schema to use for json read
    pub fn schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Configure number of max records to read for schema inference
    pub fn schema_infer_max_records(mut self, max_records: usize) -> Self {
        self.schema_infer_max_records = max_records;
        self
    }


    /// Specify the file extension for json file selection
    pub fn file_extension(mut self, file_extension: &'a str) -> Self {
        self.file_extension = file_extension;
        self
    }
}


/// Execution plan for scanning a json File
#[derive(Debug, Clone)]
pub struct JsonExec {
    /// Path to directory containing partitioned Json files with the same schema
    path: String,
    /// The individual files under path
    filenames: Vec<String>,
    /// Schema representing the Json File
    schema: SchemaRef,
    /// File extension
    file_extension: String,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Schema after the projection has been applied
    projected_schema: SchemaRef,
    /// Batch size
    batch_size: usize,
    /// Limit in nr. of rows
    limit: Option<usize>,
}

impl JsonExec {
    pub fn try_new(
        path: &str,
        options: JsonReadOptions,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        let file_extension = String::from(options.file_extension);

        let mut filenames: Vec<String> = vec![];
        common::build_file_list(path, &mut filenames, file_extension.as_str())?;
        if filenames.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "No files found at {path} with file extension {file_extension}",
                path = path,
                file_extension = file_extension.as_str()
            )));
        }

        let schema = match options.schema {
            Some(s) => s.clone(),
            None => JsonExec::try_infer_schema(&filenames, &options)?,
        };

        let projected_schema = match &projection {
            None => schema.clone(),
            Some(p) => Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()),
        };

        Ok(Self {
            path: path.to_string(),
            filenames,
            schema: Arc::new(schema),
            file_extension,
            projection,
            projected_schema: Arc::new(projected_schema),
            batch_size,
            limit,
        })
    }


    /// Infer schema for given Json dataset
    /// TODO: now we read `schema_infer_max_records` in every json file
    pub fn try_infer_schema(
        filenames: &[String],
        options: &JsonReadOptions,
    ) -> Result<Schema> {
        let mut schemas = vec![];
        let mut records_to_read = options.schema_infer_max_records;

        for fname in filenames.iter() {
            let file = File::open(fname).unwrap();
            let mut reader = BufReader::new(file);
            let schema = json::reader::infer_json_schema_from_seekable(&mut reader, Some(records_to_read)).unwrap();
            schemas.push(schema.clone());
        }
        Ok(Schema::try_merge(schemas)?)
    }


    /// Path to directory containing partitioned JSON files with the same schema
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The individual files under path
    pub fn filenames(&self) -> &[String] {
        &self.filenames
    }

    /// The File extension, ".json" by default
    pub fn file_extension(&self) -> &str {
        &self.file_extension
    }

    /// Optional projection for which columns to load
    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }

    /// Batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Limit
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }
}

#[async_trait]
impl ExecutionPlan for JsonExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the output schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.filenames.len())
    }

    /// This is a leaf node and has no children
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(JsonStream::try_new(
            &self.filenames[partition],
            self.schema.clone(),
            &self.projection,
            self.batch_size,
            self.limit,
        )?))
    }
}

struct JsonStream {
    reader: json::Reader<File>,
}

impl JsonStream {
    /// Create an iterator for a json file
    pub fn try_new(
        filename: &str,
        schema: SchemaRef,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        let file = File::open(filename)?;


        let projection_str = match projection {
            Some(fields) => Some(fields.iter().map(|x| {
                schema.field(*x).name().clone()
            }).collect()),
            None => None,
        };

        let reader = json::Reader::new(
            file,
            schema,
            batch_size,
            projection_str,
        );

        Ok(Self { reader })
    }
}


impl Stream for JsonStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let result = self.reader.next().unwrap();

        let ret = match result {
            Some(batch) => Some(Ok(batch)),
            None => None,
        };
        Poll::Ready(ret)
    }
}

impl RecordBatchStream for JsonStream {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}