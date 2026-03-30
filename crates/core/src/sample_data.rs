use std::sync::Arc;

use arrow::array::{BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

pub fn web_analytics_schema() -> Schema {
    Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("page_url", DataType::Utf8, false),
        Field::new("user_id", DataType::Int64, true),
        Field::new("country", DataType::Utf8, false),
        Field::new("duration_ms", DataType::Int32, true),
        Field::new("timestamp_us", DataType::Int64, false),
        Field::new("is_mobile", DataType::Boolean, false),
    ])
}

pub fn create_sample_batch() -> RecordBatch {
    let schema = Arc::new(web_analytics_schema());

    let event_ids = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]));

    let page_urls = Arc::new(StringArray::from(vec![
        "/home",
        "/products",
        "/home",
        "/checkout",
        "/products",
        "/home",
        "/about",
        "/products",
    ]));

    let user_ids = Arc::new(Int64Array::from(vec![
        Some(1001),
        Some(1002),
        None,
        Some(1001),
        Some(1003),
        None,
        Some(1002),
        Some(1004),
    ]));

    let countries = Arc::new(StringArray::from(vec![
        "US", "DE", "US", "JP", "US", "DE", "US", "JP",
    ]));

    let durations = Arc::new(Int32Array::from(vec![
        Some(250),
        None,
        Some(120),
        Some(890),
        Some(340),
        Some(150),
        None,
        Some(200),
    ]));

    let timestamps = Arc::new(Int64Array::from(vec![
        1705312200000000_i64,
        1705312260000000,
        1705312320000000,
        1705312380000000,
        1705312440000000,
        1705312500000000,
        1705312560000000,
        1705312620000000,
    ]));

    let is_mobile = Arc::new(BooleanArray::from(vec![
        true, false, true, false, true, true, false, true,
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            event_ids, page_urls, user_ids, countries, durations, timestamps, is_mobile,
        ],
    )
    .expect("schema and data mismatch")
}
