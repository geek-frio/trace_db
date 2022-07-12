use std::cell::Cell;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::Once;
use tantivy::schema::{Schema, INDEXED, STORED, STRING, TEXT};

pub const ZONE: &'static str = "zone";
pub const API_ID: &'static str = "api_id";
pub const SERVICE: &'static str = "service";
pub const BIZTIME: &'static str = "biztime";
pub const TRACE_ID: &'static str = "trace_id";
pub const SEGID: &'static str = "seg_id";
pub const PAYLOAD: &'static str = "payload";

/// Used for static init Tantivy Schema
pub static TRACING_SCHEMA: SCHEMA = SCHEMA {
    _private_fields: (),
};

unsafe impl Sync for Lazy {}

pub struct SCHEMA {
    _private_fields: (),
}

pub struct Lazy(Cell<MaybeUninit<Schema>>);

impl Deref for SCHEMA {
    type Target = Schema;

    fn deref(&self) -> &Schema {
        static ONCE: Once = Once::new();
        static LAZY: Lazy = Lazy(Cell::new(MaybeUninit::uninit()));

        ONCE.call_once(|| {
            let mut schema_builder = Schema::builder();
            schema_builder.add_text_field(ZONE, STRING);
            schema_builder.add_i64_field(API_ID, INDEXED);
            schema_builder.add_text_field(SERVICE, TEXT);
            schema_builder.add_u64_field(BIZTIME, STORED);
            schema_builder.add_text_field(TRACE_ID, STRING | STORED);
            schema_builder.add_text_field(SEGID, STRING);
            schema_builder.add_text_field(PAYLOAD, STRING);

            let schema = schema_builder.build();
            LAZY.0.set(MaybeUninit::new(schema));
        });

        unsafe { &(*(&*LAZY.0.as_ptr()).as_ptr()) }
    }
}
