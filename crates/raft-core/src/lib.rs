use std::collections::{BTreeMap, VecDeque};
use std::fmt;

pub type ClientId = u64;
pub type Clock = u64;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OpId {
    pub client: ClientId,
    pub clock: Clock,
}

impl OpId {
    pub fn new(client: ClientId, clock: Clock) -> Result<Self, CrdtError> {
        if client == 0 {
            return Err(CrdtError::InvalidClientId);
        }

        Ok(Self { client, clock })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpContent {
    Text(String),
    Bytes(Vec<u8>),
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Operation {
    pub id: OpId,
    pub origin_left: Option<OpId>,
    pub origin_right: Option<OpId>,
    pub content: OpContent,
    pub deleted: bool,
}

impl Operation {
    pub fn text(id: OpId, value: impl Into<String>) -> Self {
        Self {
            id,
            origin_left: None,
            origin_right: None,
            content: OpContent::Text(value.into()),
            deleted: false,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StateVector(BTreeMap<ClientId, Clock>);

impl StateVector {
    pub fn clock_for(&self, client: ClientId) -> Clock {
        self.0.get(&client).copied().unwrap_or_default()
    }

    pub fn observe(&mut self, id: OpId) {
        self.0
            .entry(id.client)
            .and_modify(|clock| *clock = (*clock).max(id.clock))
            .or_insert(id.clock);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Document {
    id: String,
    store: BTreeMap<OpId, Operation>,
    state_vector: StateVector,
    pending: VecDeque<Operation>,
}

impl Document {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            store: BTreeMap::new(),
            state_vector: StateVector::default(),
            pending: VecDeque::new(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn state_vector(&self) -> &StateVector {
        &self.state_vector
    }

    pub fn operations(&self) -> impl Iterator<Item = &Operation> {
        self.store.values()
    }

    pub fn apply_local(&mut self, op: Operation) -> Result<Vec<u8>, CrdtError> {
        self.integrate_operation(op.clone())?;
        encode_operations([op])
    }

    pub fn integrate_remote(&mut self, encoded: &[u8]) -> Result<(), CrdtError> {
        for op in decode_operations(encoded)? {
            self.integrate_operation(op)?;
        }

        Ok(())
    }

    pub fn diff(&self, remote: &StateVector) -> Result<Vec<u8>, CrdtError> {
        let missing = self
            .store
            .values()
            .filter(|op| op.id.clock > remote.clock_for(op.id.client))
            .cloned();

        encode_operations(missing)
    }

    pub fn encode_state(&self) -> Result<Vec<u8>, CrdtError> {
        encode_operations(self.store.values().cloned())
    }

    pub fn decode_state(id: impl Into<String>, bytes: &[u8]) -> Result<Self, CrdtError> {
        let mut doc = Self::new(id);
        doc.integrate_remote(bytes)?;
        Ok(doc)
    }

    fn integrate_operation(&mut self, op: Operation) -> Result<(), CrdtError> {
        if op.id.client == 0 {
            return Err(CrdtError::InvalidClientId);
        }

        if self.store.contains_key(&op.id) {
            return Ok(());
        }

        if !self.has_origin(op.origin_left) || !self.has_origin(op.origin_right) {
            self.pending.push_back(op);
            return Ok(());
        }

        self.state_vector.observe(op.id);
        self.store.insert(op.id, op);
        self.drain_pending();
        Ok(())
    }

    fn has_origin(&self, origin: Option<OpId>) -> bool {
        origin.map_or(true, |id| self.store.contains_key(&id))
    }

    fn drain_pending(&mut self) {
        let mut remaining = VecDeque::new();

        while let Some(op) = self.pending.pop_front() {
            if self.has_origin(op.origin_left) && self.has_origin(op.origin_right) {
                self.state_vector.observe(op.id);
                self.store.insert(op.id, op);
            } else {
                remaining.push_back(op);
            }
        }

        self.pending = remaining;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CrdtError {
    InvalidClientId,
    UnexpectedEof,
    InvalidContentType(u8),
    ContentTooLarge,
    TrailingBytes,
}

impl fmt::Display for CrdtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidClientId => write!(f, "client id must be non-zero"),
            Self::UnexpectedEof => write!(f, "encoded operation ended unexpectedly"),
            Self::InvalidContentType(value) => write!(f, "invalid operation content type: {value}"),
            Self::ContentTooLarge => write!(f, "operation content is too large"),
            Self::TrailingBytes => write!(f, "encoded operation contained trailing bytes"),
        }
    }
}

impl std::error::Error for CrdtError {}

pub fn encode_operations<I>(ops: I) -> Result<Vec<u8>, CrdtError>
where
    I: IntoIterator<Item = Operation>,
{
    let ops = ops.into_iter().collect::<Vec<_>>();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(ops.len() as u32).to_be_bytes());

    for op in ops {
        write_op_id(&mut bytes, op.id);
        write_optional_op_id(&mut bytes, op.origin_left);
        write_optional_op_id(&mut bytes, op.origin_right);
        bytes.push(u8::from(op.deleted));

        let (content_type, payload) = match op.content {
            OpContent::Text(value) => (1, value.into_bytes()),
            OpContent::Bytes(value) => (2, value),
            OpContent::Delete => (3, Vec::new()),
        };

        let content_len = u32::try_from(payload.len()).map_err(|_| CrdtError::ContentTooLarge)?;
        bytes.push(content_type);
        bytes.extend_from_slice(&content_len.to_be_bytes());
        bytes.extend_from_slice(&payload);
    }

    Ok(bytes)
}

pub fn decode_operations(bytes: &[u8]) -> Result<Vec<Operation>, CrdtError> {
    let mut cursor = Cursor::new(bytes);
    let count = cursor.u32()? as usize;
    let mut ops = Vec::with_capacity(count);

    for _ in 0..count {
        let id = cursor.op_id()?;
        let origin_left = cursor.optional_op_id()?;
        let origin_right = cursor.optional_op_id()?;
        let deleted = cursor.u8()? != 0;
        let content_type = cursor.u8()?;
        let content_len = cursor.u32()? as usize;
        let payload = cursor.bytes(content_len)?;

        let content = match content_type {
            1 => OpContent::Text(String::from_utf8_lossy(payload).into_owned()),
            2 => OpContent::Bytes(payload.to_vec()),
            3 => OpContent::Delete,
            value => return Err(CrdtError::InvalidContentType(value)),
        };

        ops.push(Operation {
            id,
            origin_left,
            origin_right,
            content,
            deleted,
        });
    }

    if !cursor.is_done() {
        return Err(CrdtError::TrailingBytes);
    }

    Ok(ops)
}

fn write_op_id(bytes: &mut Vec<u8>, id: OpId) {
    bytes.extend_from_slice(&id.client.to_be_bytes());
    bytes.extend_from_slice(&id.clock.to_be_bytes());
}

fn write_optional_op_id(bytes: &mut Vec<u8>, id: Option<OpId>) {
    let id = id.unwrap_or(OpId {
        client: 0,
        clock: 0,
    });
    write_op_id(bytes, id);
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_done(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn u8(&mut self) -> Result<u8, CrdtError> {
        Ok(self.bytes(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, CrdtError> {
        let bytes = self.bytes(4)?;
        Ok(u32::from_be_bytes(
            bytes.try_into().expect("slice length checked"),
        ))
    }

    fn u64(&mut self) -> Result<u64, CrdtError> {
        let bytes = self.bytes(8)?;
        Ok(u64::from_be_bytes(
            bytes.try_into().expect("slice length checked"),
        ))
    }

    fn op_id(&mut self) -> Result<OpId, CrdtError> {
        let client = self.u64()?;
        let clock = self.u64()?;
        OpId::new(client, clock)
    }

    fn optional_op_id(&mut self) -> Result<Option<OpId>, CrdtError> {
        let client = self.u64()?;
        let clock = self.u64()?;

        if client == 0 {
            Ok(None)
        } else {
            Ok(Some(OpId::new(client, clock)?))
        }
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8], CrdtError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(CrdtError::UnexpectedEof)?;

        if end > self.bytes.len() {
            return Err(CrdtError::UnexpectedEof);
        }

        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_and_decodes_operations() {
        let op = Operation::text(OpId::new(7, 1).unwrap(), "hello");

        let bytes = encode_operations([op.clone()]).unwrap();
        assert_eq!(decode_operations(&bytes).unwrap(), vec![op]);
    }

    #[test]
    fn document_ignores_duplicate_operations() {
        let op = Operation::text(OpId::new(1, 1).unwrap(), "a");
        let mut doc = Document::new("doc");
        let update = doc.apply_local(op).unwrap();

        doc.integrate_remote(&update).unwrap();

        assert_eq!(doc.operations().count(), 1);
        assert_eq!(doc.state_vector().clock_for(1), 1);
    }

    #[test]
    fn diff_returns_operations_missing_from_remote_state_vector() {
        let mut doc = Document::new("doc");
        doc.apply_local(Operation::text(OpId::new(1, 1).unwrap(), "a"))
            .unwrap();
        doc.apply_local(Operation::text(OpId::new(1, 2).unwrap(), "b"))
            .unwrap();

        let mut remote = StateVector::default();
        remote.observe(OpId::new(1, 1).unwrap());

        let diff = decode_operations(&doc.diff(&remote).unwrap()).unwrap();

        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].id, OpId::new(1, 2).unwrap());
    }

    #[test]
    fn queues_operations_until_origins_arrive() {
        let first = Operation::text(OpId::new(1, 1).unwrap(), "a");
        let mut second = Operation::text(OpId::new(1, 2).unwrap(), "b");
        second.origin_left = Some(first.id);

        let mut doc = Document::new("doc");
        doc.integrate_operation(second).unwrap();
        assert_eq!(doc.operations().count(), 0);

        doc.integrate_operation(first).unwrap();
        assert_eq!(doc.operations().count(), 2);
    }
}
