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

use arrow::datatypes::{Schema, SchemaRef};
use arrow::json;

use crate::error::{DataFusionError, Result};
use crate::physical_plan::common;

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
    // pub fn try_new(
    //     path: &str,
    //     options: JsonReadOptions,
    //     projection: Option<Vec<usize>>,
    //     batch_size: usize,
    //     limit: Option<usize>,
    // ) -> Result<Self> {
    //     let file_extension = String::from(options.file_extension);
    //
    //     let mut filenames: Vec<String> = vec![];
    //     common::build_file_list(path, &mut filenames, file_extension.as_str())?;
    //     if filenames.is_empty() {
    //         return Err(DataFusionError::Execution(format!(
    //             "No files found at {$path} with file extension {$file_extension}",
    //             path = path,
    //             file_extension = file_extension.as_str()
    //         )));
    //     }
    //
    //     let schema = match options.schema {
    //         Some(s) => s.clone(),
    //         None => JsonExec::try
    //     }
    // }


    pub fn try_infer_schema(
        filenames: &[String],
        optionsL &JsonReadOptions,
    ) -> Result<Schema> {
        Ok(json::reader::infer_json_schema())
    }
}