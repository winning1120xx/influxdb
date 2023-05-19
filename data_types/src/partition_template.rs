use generated_types::influxdata::iox::partition_template::v1 as proto;
use once_cell::sync::Lazy;
use std::sync::Arc;

/// Allocationless and protobufless access to the parts of a template needed to actually do
/// partitioning.
#[derive(Debug)]
#[allow(missing_docs)]
pub enum TemplatePart<'a> {
    TagValue(&'a str),
    TimeFormat(&'a str),
}

/// The default partitioning scheme is by each day according to the "time" column.
pub static PARTITION_BY_DAY_PROTO: Lazy<Arc<proto::PartitionTemplate>> = Lazy::new(|| {
    Arc::new(proto::PartitionTemplate {
        parts: vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TimeFormat(
                "%Y-%m-%d".to_owned(),
            )),
        }],
    })
});

/// A partition template specified by a namespace record.
#[derive(Debug, PartialEq, Clone, sqlx::Type)]
#[sqlx(transparent)]
pub struct NamespacePartitionTemplateOverride(SerializationWrapper);

impl Default for NamespacePartitionTemplateOverride {
    fn default() -> Self {
        Self(SerializationWrapper(Arc::clone(&PARTITION_BY_DAY_PROTO)))
    }
}

impl From<proto::PartitionTemplate> for NamespacePartitionTemplateOverride {
    fn from(partition_template: proto::PartitionTemplate) -> Self {
        Self(SerializationWrapper(Arc::new(partition_template)))
    }
}

/// When a table is being created implicitly by a write, there is no possibility of a user-supplied
/// partition template, so the table will get the namespace's partition template.
impl From<&NamespacePartitionTemplateOverride> for TablePartitionTemplateOverride {
    fn from(namespace_template: &NamespacePartitionTemplateOverride) -> Self {
        Self(SerializationWrapper(Arc::clone(&namespace_template.0 .0)))
    }
}

/// A partition template specified by a table record.
#[derive(Debug, PartialEq, Clone, sqlx::Type)]
#[sqlx(transparent)]
pub struct TablePartitionTemplateOverride(SerializationWrapper);

impl Default for TablePartitionTemplateOverride {
    fn default() -> Self {
        Self(SerializationWrapper(Arc::clone(&PARTITION_BY_DAY_PROTO)))
    }
}

impl TablePartitionTemplateOverride {
    /// When a table is being explicitly created, the creation request might have contained a
    /// custom partition template for that table. If the custom partition template is present, use
    /// it. Otherwise, use the namespace's partition template.
    #[allow(dead_code)] // This will be used by the as-yet unwritten create table gRPC API.
    pub fn new(
        custom_table_template: Option<proto::PartitionTemplate>,
        namespace_template: &NamespacePartitionTemplateOverride,
    ) -> Self {
        custom_table_template
            .map(Arc::new)
            .map(SerializationWrapper)
            .map(Self)
            .unwrap_or_else(|| namespace_template.into())
    }

    /// Iterate through the protobuf parts and lend out what the `mutable_batch` crate needs to
    /// build `PartitionKey`s.
    pub fn parts(&self) -> impl Iterator<Item = TemplatePart<'_>> {
        self.0
             .0
            .parts
            .iter()
            .flat_map(|part| part.part.as_ref())
            .map(|part| match part {
                proto::template_part::Part::TagValue(value) => TemplatePart::TagValue(value),
                proto::template_part::Part::TimeFormat(fmt) => TemplatePart::TimeFormat(fmt),
            })
    }
}

/// This manages the serialization/deserialization of the `proto::PartitionTemplate` type to and
/// from the database through `sqlx` for the `NamespacePartitionTemplateOverride` and
/// `TablePartitionTemplateOverride` types. It's an internal implementation detail to minimize code
/// duplication.
#[derive(Debug, Clone, PartialEq)]
struct SerializationWrapper(Arc<proto::PartitionTemplate>);

impl<DB> sqlx::Type<DB> for SerializationWrapper
where
    sqlx::types::Json<Self>: sqlx::Type<DB>,
    DB: sqlx::Database,
{
    fn type_info() -> DB::TypeInfo {
        <sqlx::types::Json<Self> as sqlx::Type<DB>>::type_info()
    }
}

impl<'q, DB> sqlx::Encode<'q, DB> for SerializationWrapper
where
    DB: sqlx::Database,
    for<'b> sqlx::types::Json<&'b proto::PartitionTemplate>: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        <sqlx::types::Json<&proto::PartitionTemplate> as sqlx::Encode<'_, DB>>::encode_by_ref(
            &sqlx::types::Json(&self.0),
            buf,
        )
    }
}

impl<'q, DB> sqlx::Decode<'q, DB> for SerializationWrapper
where
    DB: sqlx::Database,
    sqlx::types::Json<proto::PartitionTemplate>: sqlx::Decode<'q, DB>,
{
    fn decode(
        value: <DB as sqlx::database::HasValueRef<'q>>::ValueRef,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        Ok(Self(
            <sqlx::types::Json<proto::PartitionTemplate> as sqlx::Decode<'_, DB>>::decode(value)?
                .0
                .into(),
        ))
    }
}

/// In production code, the template should come from protobuf that is either from the database or
/// from a gRPC request. In tests, building protobuf is painful, so here's an easier way to create
/// a `TablePartitionTemplateOverride`.
pub fn test_table_partition_override(
    parts: Vec<TemplatePart<'static>>,
) -> TablePartitionTemplateOverride {
    let parts = parts
        .into_iter()
        .map(|part| {
            let part = match part {
                TemplatePart::TagValue(value) => proto::template_part::Part::TagValue(value.into()),
                TemplatePart::TimeFormat(fmt) => proto::template_part::Part::TimeFormat(fmt.into()),
            };

            proto::TemplatePart { part: Some(part) }
        })
        .collect();

    let proto = Arc::new(proto::PartitionTemplate { parts });
    TablePartitionTemplateOverride(SerializationWrapper(proto))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use sqlx::Encode;

    /// This test asserts the default derived partitioning scheme with no
    /// overrides.
    ///
    /// Changing this default during the lifetime of a cluster will cause the
    /// implicit (not overridden) partition schemes to change, potentially
    /// breaking the system invariant that a given primary keys maps to a
    /// single partition.
    ///
    /// You shouldn't be changing this!
    #[test]
    fn test_default_template_fixture() {
        let ns = NamespacePartitionTemplateOverride::default();
        let table = TablePartitionTemplateOverride::new(None, &ns);
        let got = table.parts().collect::<Vec<_>>();
        assert_matches!(got.as_slice(), [TemplatePart::TimeFormat("%Y-%m-%d")]);
    }

    #[test]
    fn no_custom_table_template_specified_gets_namespace_template() {
        let namespace_template =
            NamespacePartitionTemplateOverride::from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                }],
            });
        let table_template = TablePartitionTemplateOverride::new(None, &namespace_template);

        assert_eq!(table_template.0, namespace_template.0);
    }

    #[test]
    fn custom_table_template_specified_ignores_namespace_template() {
        let custom_table_template = proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue("region".into())),
            }],
        };
        let namespace_template =
            NamespacePartitionTemplateOverride::from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                }],
            });
        let table_template = TablePartitionTemplateOverride::new(
            Some(custom_table_template.clone()),
            &namespace_template,
        );

        assert_eq!(table_template.0 .0.as_ref(), &custom_table_template);
    }

    // The JSON representation of the partition template protobuf is stored in the database, so
    // the encode/decode implementations need to be stable if we want to avoid having to
    // migrate the values stored in the database.

    #[test]
    fn proto_encode_json_stability() {
        let custom_template = proto::PartitionTemplate {
            parts: vec![
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("region".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                },
            ],
        };
        let expected_json_str = "{\"parts\":[\
            {\"tagValue\":\"region\"},\
            {\"timeFormat\":\"year-%Y\"}\
        ]}";

        let namespace = NamespacePartitionTemplateOverride::from(custom_template);
        let mut buf = Default::default();
        let _ = <NamespacePartitionTemplateOverride as Encode<'_, sqlx::Sqlite>>::encode_by_ref(
            &namespace, &mut buf,
        );

        fn extract_sqlite_argument_text(
            argument_value: &sqlx::sqlite::SqliteArgumentValue,
        ) -> String {
            match argument_value {
                sqlx::sqlite::SqliteArgumentValue::Text(cow) => cow.to_string(),
                other => panic!("Expected Text values, got: {other:?}"),
            }
        }

        let namespace_json_str: String = buf.iter().map(extract_sqlite_argument_text).collect();
        assert_eq!(namespace_json_str, expected_json_str);

        let table = TablePartitionTemplateOverride::from(&namespace);
        let mut buf = Default::default();
        let _ = <TablePartitionTemplateOverride as Encode<'_, sqlx::Sqlite>>::encode_by_ref(
            &table, &mut buf,
        );
        let table_json_str: String = buf.iter().map(extract_sqlite_argument_text).collect();
        assert_eq!(table_json_str, expected_json_str);
    }
}
