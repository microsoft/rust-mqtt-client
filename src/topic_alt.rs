
use crate::mqtt_proto;
use crate::buffer_pool;
use crate::buffer_pool::BufferPool;

pub struct TopicName(mqtt_proto::Topic<mqtt_proto::ByteStr<buffer_pool::SharedImpl>>);

impl TopicName {
    pub fn new<S>(s: S) -> Result<Self, mqtt_proto::DecodeError>
    where
        S: AsRef<str>,
    {
        let mut o = buffer_pool::BufferPoolImpl.take_empty_owned();
        let bs = mqtt_proto::ByteStr::new(&mut o, &s).unwrap();
        let topic = mqtt_proto::Topic::new(bs)?;
        Ok(TopicName(topic))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    // pub fn as_str(&self) -> &str {
    //     self.0.as_str()
    // }
}