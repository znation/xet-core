use std::fmt::Display;

use crate::error::CfgError;
use config::{ConfigError, Map, Value, ValueKind};
use serde::{ser, Serialize};

/// Most of the implementation was copied from config::ser::ConfigSerializer,
/// which is unfortunately private.
///
/// Key changes are to make `output` a string instead of a config::Config
/// object and to ignore empty values in the `serialize_primative` function.
#[derive(Default, Debug)]
pub struct ConfigSerializer {
    keys: Vec<(String, Option<usize>)>,
    output: String,
    kv_pairs: Map<String, Value>,
}

type Result<T> = std::result::Result<T, ConfigError>;

/// Serializes the indicated value into a string of k=v pairs.
pub fn to_string<T: Serialize>(value: &T) -> std::result::Result<String, CfgError> {
    let mut serializer = ConfigSerializer::default();
    value.serialize(&mut serializer)?;
    Ok(serializer.output)
}

pub fn to_map<T: Serialize>(value: &T) -> std::result::Result<Map<String, Value>, CfgError> {
    let mut serializer = ConfigSerializer::default();
    value.serialize(&mut serializer)?;
    Ok(serializer.kv_pairs)
}

impl ConfigSerializer {
    fn serialize_primitive<T>(&mut self, value: T) -> Result<()>
    where
        T: Into<Value> + Display,
    {
        let key = match self.last_key_index_pair() {
            Some((key, Some(index))) => format!("{key}[{index}]"),
            Some((key, None)) => key.to_string(),
            None => {
                return Err(ConfigError::Message(format!(
                    "key is not found for value {value}"
                )))
            }
        };
        let value_str = value.to_string();
        let v: Value = value.into();
        if v.kind == ValueKind::Nil || value_str.is_empty() {
            return Ok(());
        }
        let output_str = format!("{}={}\n", &key, value_str);
        self.output += output_str.as_str();
        self.kv_pairs.insert(key, v);
        Ok(())
    }

    fn last_key_index_pair(&self) -> Option<(&str, Option<usize>)> {
        let len = self.keys.len();
        if len > 0 {
            self.keys
                .get(len - 1)
                .map(|&(ref key, opt)| (key.as_str(), opt))
        } else {
            None
        }
    }

    fn inc_last_key_index(&mut self) -> Result<()> {
        let len = self.keys.len();
        if len > 0 {
            self.keys
                .get_mut(len - 1)
                .map(|pair| pair.1 = pair.1.map(|i| i + 1).or(Some(0)))
                .ok_or_else(|| ConfigError::Message(format!("last key is not found in {len} keys")))
        } else {
            Err(ConfigError::Message("keys is empty".to_string()))
        }
    }

    fn make_full_key(&self, key: &str) -> String {
        let len = self.keys.len();
        if len > 0 {
            if let Some(&(ref prev_key, index)) = self.keys.get(len - 1) {
                return if let Some(index) = index {
                    format!("{prev_key}[{index}].{key}")
                } else {
                    format!("{prev_key}.{key}")
                };
            }
        }
        key.to_string()
    }

    fn push_key(&mut self, key: &str) {
        let full_key = self.make_full_key(key);
        self.keys.push((full_key, None));
    }

    fn pop_key(&mut self) -> Option<(String, Option<usize>)> {
        self.keys.pop()
    }
}

impl<'a> ser::Serializer for &'a mut ConfigSerializer {
    type Ok = ();
    type Error = ConfigError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok> {
        self.serialize_primitive(v)
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok> {
        self.serialize_i64(v.into())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok> {
        self.serialize_i64(v.into())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok> {
        self.serialize_i64(v.into())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok> {
        self.serialize_primitive(v)
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok> {
        self.serialize_u64(v.into())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok> {
        self.serialize_u64(v.into())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok> {
        self.serialize_u64(v.into())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok> {
        if v > (i64::MAX as u64) {
            Err(ConfigError::Message(format!(
                "value {} is greater than the max {}",
                v,
                i64::MAX
            )))
        } else {
            self.serialize_i64(v as i64)
        }
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok> {
        self.serialize_f64(v.into())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok> {
        self.serialize_primitive(v)
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok> {
        self.serialize_primitive(v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok> {
        self.serialize_primitive(v.to_string())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok> {
        use serde::ser::SerializeSeq;
        let mut seq = self.serialize_seq(Some(v.len()))?;
        for byte in v {
            seq.serialize_element(byte)?;
        }
        seq.end()
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        self.serialize_primitive(Value::from(ValueKind::Nil))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok>
    where
        T: ?Sized + ser::Serialize,
    {
        self.push_key(variant);
        value.serialize(&mut *self)?;
        self.pop_key();
        Ok(())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.push_key(variant);
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(self)
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        self.push_key(variant);
        Ok(self)
    }
}

impl<'a> ser::SerializeSeq for &'a mut ConfigSerializer {
    type Ok = ();
    type Error = ConfigError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.inc_last_key_index()?;
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut ConfigSerializer {
    type Ok = ();
    type Error = ConfigError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.inc_last_key_index()?;
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut ConfigSerializer {
    type Ok = ();
    type Error = ConfigError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.inc_last_key_index()?;
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut ConfigSerializer {
    type Ok = ();
    type Error = ConfigError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.inc_last_key_index()?;
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        self.pop_key();
        Ok(())
    }
}

impl<'a> ser::SerializeMap for &'a mut ConfigSerializer {
    type Ok = ();
    type Error = ConfigError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        let key_serializer = StringKeySerializer;
        let key = key.serialize(key_serializer)?;
        self.push_key(&key);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)?;
        self.pop_key();
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> ser::SerializeStruct for &'a mut ConfigSerializer {
    type Ok = ();
    type Error = ConfigError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.push_key(key);
        value.serialize(&mut **self)?;
        self.pop_key();
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> ser::SerializeStructVariant for &'a mut ConfigSerializer {
    type Ok = ();
    type Error = ConfigError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.push_key(key);
        value.serialize(&mut **self)?;
        self.pop_key();
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        self.pop_key();
        Ok(())
    }
}

pub struct StringKeySerializer;

impl ser::Serializer for StringKeySerializer {
    type Ok = String;
    type Error = ConfigError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok> {
        Ok(String::from_utf8_lossy(v).to_string())
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        Ok(String::new())
    }

    fn serialize_unit_struct(self, _name: &str) -> Result<Self::Ok> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &str,
        _variant_index: u32,
        variant: &str,
    ) -> Result<Self::Ok> {
        Ok(variant.to_string())
    }

    fn serialize_newtype_struct<T>(self, _name: &str, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &str,
        _variant_index: u32,
        _variant: &str,
        value: &T,
    ) -> Result<Self::Ok>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Err(ConfigError::Message(
            "seq can't serialize to string key".to_string(),
        ))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Err(ConfigError::Message(
            "tuple can't serialize to string key".to_string(),
        ))
    }

    fn serialize_tuple_struct(self, name: &str, _len: usize) -> Result<Self::SerializeTupleStruct> {
        Err(ConfigError::Message(format!(
            "tuple struct {name} can't serialize to string key"
        )))
    }

    fn serialize_tuple_variant(
        self,
        name: &str,
        _variant_index: u32,
        variant: &str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(ConfigError::Message(format!(
            "tuple variant {name}::{variant} can't serialize to string key"
        )))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(ConfigError::Message(
            "map can't serialize to string key".to_string(),
        ))
    }

    fn serialize_struct(self, name: &str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(ConfigError::Message(format!(
            "struct {name} can't serialize to string key"
        )))
    }

    fn serialize_struct_variant(
        self,
        name: &str,
        _variant_index: u32,
        variant: &str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(ConfigError::Message(format!(
            "struct variant {name}::{variant} can't serialize to string key"
        )))
    }
}

impl ser::SerializeSeq for StringKeySerializer {
    type Ok = String;
    type Error = ConfigError;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

impl ser::SerializeTuple for StringKeySerializer {
    type Ok = String;
    type Error = ConfigError;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

impl ser::SerializeTupleStruct for StringKeySerializer {
    type Ok = String;
    type Error = ConfigError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

impl ser::SerializeTupleVariant for StringKeySerializer {
    type Ok = String;
    type Error = ConfigError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

impl ser::SerializeMap for StringKeySerializer {
    type Ok = String;
    type Error = ConfigError;

    fn serialize_key<T>(&mut self, _key: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

impl ser::SerializeStruct for StringKeySerializer {
    type Ok = String;
    type Error = ConfigError;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

impl ser::SerializeStructVariant for StringKeySerializer {
    type Ok = String;
    type Error = ConfigError;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::cfg;
    use crate::cfg::Cfg;

    #[test]
    fn test_struct() {
        let mut cfg = Cfg::with_default_values();
        cfg.cache.as_mut().unwrap().path = Some("/tmp/cache".into());
        let data = to_string(&cfg).unwrap();
        let expected = format!(
            r#"version=1
cache.path=/tmp/cache
cache.size={}
log.level={}
axe.enabled=true
axe.axe_code=5454
"#,
            cfg::DEFAULT_CACHE_SIZE,
            cfg::DEFAULT_LOG_LEVEL,
        );
        assert_eq!(data, expected);
    }

    #[test]
    fn test_empty() {
        let cfg = Cfg::default();
        let data = to_string(&cfg).unwrap();
        assert_eq!(data, "version=1\n");
    }
}
