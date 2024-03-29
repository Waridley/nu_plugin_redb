use std::error::Error;
use nu_plugin::{EvaluatedCall, LabeledError, Plugin};
use nu_protocol::{Category, PluginSignature, Span, Type, Value};
use redb::backends::InMemoryBackend;
use redb::{Builder, Database, ReadableTable, ReadOnlyTable, ReadTransaction, RedbKey, RedbValue, StorageBackend, TableDefinition, TableError, TableHandle, TypeName, UntypedTableHandle};

pub struct RedbPlugin {}

impl RedbPlugin {
	pub fn parse(self, value: &Value) -> Result<Value, LabeledError> {
		let span = value.span();
		let bytes = value.as_binary()?;
		let mut backend = InMemoryBackend::new();
		backend.set_len(bytes.len() as u64)
			.map_err(|e| labeled_err("Error setting InMemoryBackend length", e, span))?;
		backend.write(0, bytes)
			.map_err(|e| labeled_err("Error writing bytes to InMemoryBackend", e, span))?;
		
		let db = Builder::new().create_with_backend(backend)
			.map_err(|e| labeled_err("Error loading Database from file contents as InMemoryBackend", e, span))?;
		
		let db = db.begin_read()
			.map_err(|e| labeled_err("Error beginning read transaction", e, span))?;
		
		let val = db.list_tables()
			.map_err(|e| labeled_err("Error listing tables", e, span))?
			.map(|handle| open_table(&db, handle, span))
			.collect();
		
		let tables = Value::record(val, span);
		
		
		
		let ret = Value::record([
			("tables".to_string(), tables),
			("multimap_tables".to_string(), Value::string("TODO", span)),
		].into_iter().collect(), span);
		
		Ok(ret)
	}
}

fn labeled_err(label: impl Into<String>, e: impl Error, span: Span) -> LabeledError {
	LabeledError {
		label: label.into(),
		msg: format!("{e}"),
		span: Some(span),
	}
}

fn open_table(db: &ReadTransaction, handle: UntypedTableHandle, span: Span) -> (String, Value) {
	let name = handle.name();
	let first_guess  = db.open_table(TableDefinition::<&str, &[u8]>::new(name));
	
	let table = match first_guess {
		Ok(table) => table_as_record(table, span),
		Err(TableError::TableTypeMismatch { key, value, .. }) => {
			let table = if key == <&str>::type_name() {
				guess::<&str, &str>(db, name, &key, &value, span)
					.or_else(|| guess_record::<bool>(db, name, &value, span))
					.or_else(|| guess_record::<char>(db, name, &value, span))
					.or_else(|| guess_record::<f32>(db, name, &value, span))
					.or_else(|| guess_record::<f64>(db, name, &value, span))
					.or_else(|| guess_record::<i8>(db, name, &value, span))
					.or_else(|| guess_record::<i16>(db, name, &value, span))
					.or_else(|| guess_record::<i32>(db, name, &value, span))
					.or_else(|| guess_record::<i64>(db, name, &value, span))
					.or_else(|| guess_record::<i128>(db, name, &value, span))
					.or_else(|| guess_record::<u8>(db, name, &value, span))
					.or_else(|| guess_record::<u16>(db, name, &value, span))
					.or_else(|| guess_record::<u32>(db, name, &value, span))
					.or_else(|| guess_record::<u64>(db, name, &value, span))
					.or_else(|| guess_record::<u128>(db, name, &value, span))
			} else {
				let mut table = None;
				macro_rules! guess_all_pairs {
			    // The end of iteration: we exhausted the first list
			    ([] => [$($y:ty,)*]) => {};
			
			    // The head/tail recursion: pick the first element of the first list
			    // and recursively do it for the tail.
			    ([$head:ty, $($tail:ty,)*] => [$($y:ty,)*]) => {
			        $(
			            table = table.or_else(|| guess::<$head, $y>(db, name, &key, &value, span));
			        )*
			        guess_all_pairs!([$($tail,)*] => [$($y,)*]);
			    };
				}
				guess_all_pairs!(
					[bool, char, i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, (),]
					=> [&str, bool, char, f32, f64, i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, (),]
				);
				table
			};
			
			table.unwrap_or_else(|| Value::string("<unknown type>", span))
		}
		Err(e) => {
			Value::string(format!("{e}"), span)
		}
	};
	
	(name.to_string(), table)
}

fn guess<K: RedbKey + 'static, V: RedbValue + 'static>(db: &ReadTransaction, name: &str, key: &TypeName, value: &TypeName, span: Span) -> Option<Value> {
	if *key == K::type_name() && *value == V::type_name() {
		db.open_table(TableDefinition::<K, V>::new(name))
		.map_err(|e| eprintln!("ERROR: {e}"))
		.ok()
		.map(|table| map_table(table, span))
	} else {
		None
	}
}

fn guess_record<V: RedbValue + 'static>(db: &ReadTransaction, name: &str, value: &TypeName, span: Span) -> Option<Value> {
	if *value == V::type_name() {
		db.open_table(TableDefinition::<&str, V>::new(name))
			.map_err(|e| eprintln!("ERROR: {e}"))
			.ok()
			.map(|table| table_as_record(table, span))
	} else {
		None
	}
}

fn table_as_record<V: RedbValue>(table: ReadOnlyTable<&str, V>, span: Span) -> Value {
	let Ok(table) = table.iter() else { return Value::string("❎ Table", span) };
	let record = table.map(|row| {
		let Ok((key, value)) = row else {
			return ("❎ Key".to_string(), Value::string("❎ Value", span))
		};
		let key = key.value();
		let value = value.value();
		
		let value = Value::string(String::from_utf8_lossy(V::as_bytes(&value).as_ref()), span);
		(key.to_string(), value)
	}).collect();
	Value::record(record, span)
}

fn map_table<K: RedbKey, V: RedbValue>(table: ReadOnlyTable<K, V>, span: Span) -> Value {
	let Ok(table) = table.iter() else { return Value::string("❎ Table", span) };
	let records = table.map(|row| {
		let Ok((key, value)) = row else {
			let record = [("key".to_string(), Value::string("❎ Key", span)), ("value".to_string(), Value::string("❎ Value", span))].into_iter().collect();
			return Value::record(record, span)
		};
		let key = key.value();
		let value = value.value();
		
		let key = Value::string(String::from_utf8_lossy(K::as_bytes(&key).as_ref()), span);
		let value = Value::string(String::from_utf8_lossy(V::as_bytes(&value).as_ref()), span);
		let record = [("key".to_string(), key), ("value".to_string(), value)].into_iter().collect();
		let record = Value::record(record, span);
		record
	}).collect();
	Value::list(records, span)
}

impl From<&Option<Value>> for RedbPlugin {
	fn from(_value: &Option<Value>) -> Self {
		Self {}
	}
}

impl Plugin for RedbPlugin {
	fn signature(&self) -> Vec<PluginSignature> {
		vec![PluginSignature::build("from redb")
			.input_output_types(vec![(Type::Binary, Type::Record(vec![]))])
			.usage("Parse .redb file and create table.")
			.category(Category::Formats)]
	}

	fn run(
		&mut self,
		name: &str,
		config: &Option<Value>,
		call: &EvaluatedCall,
		input: &Value,
	) -> Result<Value, LabeledError> {
		match name {
			"from redb" => RedbPlugin::from(config).parse(input),
			_ => Err(LabeledError {
				label: "Plugin call with wrong name signature".to_string(),
				msg: format!("RedbPlugin called with {name:?} instead of 'from redb'"),
				span: Some(call.head),
			}),
		}
	}
}
