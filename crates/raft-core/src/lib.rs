#[cfg(test)]
use std::collections::BTreeSet;
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
    pub fn from_entries<I>(entries: I) -> Result<Self, CrdtError>
    where
        I: IntoIterator<Item = (ClientId, Clock)>,
    {
        let mut vector = Self::default();

        for (client, clock) in entries {
            if client == 0 {
                return Err(CrdtError::InvalidClientId);
            }
            vector.0.insert(client, clock);
        }

        Ok(vector)
    }

    pub fn entries(&self) -> impl Iterator<Item = (ClientId, Clock)> + '_ {
        self.0.iter().map(|(client, clock)| (*client, *clock))
    }

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

    pub fn pending_len(&self) -> usize {
        self.pending.len()
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

    pub fn diff_from_encoded_state_vector(&self, remote: &[u8]) -> Result<Vec<u8>, CrdtError> {
        self.diff(&decode_state_vector(remote)?)
    }

    pub fn encode_state(&self) -> Result<Vec<u8>, CrdtError> {
        encode_operations(self.store.values().cloned())
    }

    pub fn encode_state_vector(&self) -> Result<Vec<u8>, CrdtError> {
        encode_state_vector(&self.state_vector)
    }

    pub fn decode_state(id: impl Into<String>, bytes: &[u8]) -> Result<Self, CrdtError> {
        let mut doc = Self::new(id);
        doc.integrate_remote(bytes)?;
        Ok(doc)
    }

    pub fn compact_tombstones(&mut self, stable: &StateVector) -> usize {
        let mut referenced = BTreeMap::<OpId, usize>::new();
        let mut deleted = BTreeMap::<OpId, Vec<OpId>>::new();

        for op in self.store.values() {
            if let Some(origin) = op.origin_left {
                *referenced.entry(origin).or_default() += 1;
            }
            if let Some(origin) = op.origin_right {
                *referenced.entry(origin).or_default() += 1;
            }
            if matches!(op.content, OpContent::Delete) {
                if let Some(target) = op.origin_left {
                    deleted.entry(target).or_default().push(op.id);
                }
            }
        }

        let mut remove = Vec::new();
        for (target, delete_ids) in deleted {
            let Some(target_op) = self.store.get(&target) else {
                continue;
            };
            if !matches!(target_op.content, OpContent::Text(_)) {
                continue;
            }
            if !is_stable(target, stable) || delete_ids.iter().any(|id| !is_stable(*id, stable)) {
                continue;
            }
            let target_ref_count = referenced.get(&target).copied().unwrap_or_default();
            if target_ref_count > delete_ids.len() {
                continue;
            }
            if delete_ids
                .iter()
                .any(|id| referenced.get(id).copied().unwrap_or_default() > 0)
            {
                continue;
            }

            remove.push(target);
            remove.extend(delete_ids);
        }

        let removed = remove.len();
        for id in remove {
            self.store.remove(&id);
        }
        removed
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
        loop {
            let mut progressed = false;
            let mut remaining = VecDeque::new();

            while let Some(op) = self.pending.pop_front() {
                if self.has_origin(op.origin_left) && self.has_origin(op.origin_right) {
                    self.state_vector.observe(op.id);
                    self.store.insert(op.id, op);
                    progressed = true;
                } else {
                    remaining.push_back(op);
                }
            }

            self.pending = remaining;

            if !progressed {
                return;
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextDocument {
    client_id: ClientId,
    next_clock: Clock,
    doc: Document,
}

impl TextDocument {
    pub fn new(id: impl Into<String>, client_id: ClientId) -> Result<Self, CrdtError> {
        if client_id == 0 {
            return Err(CrdtError::InvalidClientId);
        }

        Ok(Self {
            client_id,
            next_clock: 1,
            doc: Document::new(id),
        })
    }

    pub fn document(&self) -> &Document {
        &self.doc
    }

    pub fn text(&self) -> String {
        self.rga_sequence()
            .into_iter()
            .filter(|item| item.visible)
            .map(|item| item.text)
            .collect()
    }

    pub fn insert(&mut self, index: usize, value: &str) -> Result<Vec<u8>, CrdtError> {
        let items = self.visible_text_items();
        if index > items.len() {
            return Err(CrdtError::IndexOutOfBounds);
        }

        let mut ops = Vec::new();
        let mut origin_left = index
            .checked_sub(1)
            .and_then(|left| items.get(left))
            .map(|item| item.id);
        let origin_right = items.get(index).map(|item| item.id);

        for text in value.chars().map(String::from) {
            let id = self.next_id();
            let op = Operation {
                id,
                origin_left,
                origin_right,
                content: OpContent::Text(text),
                deleted: false,
            };
            self.doc.integrate_operation(op.clone())?;
            origin_left = Some(id);
            ops.push(op);
        }

        encode_operations(ops)
    }

    pub fn delete(&mut self, index: usize, len: usize) -> Result<Vec<u8>, CrdtError> {
        let items = self.visible_text_items();
        let end = index.checked_add(len).ok_or(CrdtError::IndexOutOfBounds)?;
        if end > items.len() {
            return Err(CrdtError::IndexOutOfBounds);
        }

        let mut ops = Vec::new();
        for item in &items[index..end] {
            let op = Operation {
                id: self.next_id(),
                origin_left: Some(item.id),
                origin_right: None,
                content: OpContent::Delete,
                deleted: false,
            };
            self.doc.integrate_operation(op.clone())?;
            ops.push(op);
        }

        encode_operations(ops)
    }

    pub fn apply_update(&mut self, update: &[u8]) -> Result<(), CrdtError> {
        self.doc.integrate_remote(update)?;
        self.next_clock = self
            .next_clock
            .max(self.doc.state_vector().clock_for(self.client_id) + 1);
        Ok(())
    }

    pub fn encode_state(&self) -> Result<Vec<u8>, CrdtError> {
        self.doc.encode_state()
    }

    pub fn encode_state_vector(&self) -> Result<Vec<u8>, CrdtError> {
        self.doc.encode_state_vector()
    }

    pub fn diff_from_encoded_state_vector(&self, remote: &[u8]) -> Result<Vec<u8>, CrdtError> {
        self.doc.diff_from_encoded_state_vector(remote)
    }

    pub fn compact_tombstones(&mut self, stable: &StateVector) -> usize {
        self.doc.compact_tombstones(stable)
    }

    fn next_id(&mut self) -> OpId {
        let id = OpId {
            client: self.client_id,
            clock: self.next_clock,
        };
        self.next_clock += 1;
        id
    }

    fn visible_text_items(&self) -> Vec<TextItem> {
        self.rga_sequence()
            .into_iter()
            .filter(|item| item.visible)
            .collect()
    }

    fn rga_sequence(&self) -> Vec<TextItem> {
        let deleted = self.deleted_targets();
        let mut children = BTreeMap::<Option<OpId>, Vec<&Operation>>::new();

        for op in self.doc.store.values() {
            if !matches!(op.content, OpContent::Text(_)) {
                continue;
            }
            children.entry(op.origin_left).or_default().push(op);
        }

        for ops in children.values_mut() {
            sort_rga_siblings(ops);
        }

        let mut ordered = Vec::new();
        append_rga_children(None, &children, &deleted, &mut ordered);
        ordered
    }
}

fn append_rga_children(
    parent: Option<OpId>,
    children: &BTreeMap<Option<OpId>, Vec<&Operation>>,
    deleted: &BTreeMap<OpId, OpId>,
    ordered: &mut Vec<TextItem>,
) {
    let Some(ops) = children.get(&parent) else {
        return;
    };

    for op in ops {
        let text = match &op.content {
            OpContent::Text(text) => text.clone(),
            _ => continue,
        };

        ordered.push(TextItem {
            id: op.id,
            origin_left: op.origin_left,
            origin_right: op.origin_right,
            text,
            visible: !op.deleted && !deleted.contains_key(&op.id),
        });
        append_rga_children(Some(op.id), children, deleted, ordered);
    }
}

fn sort_rga_siblings(ops: &mut Vec<&Operation>) {
    let mut sorted = Vec::<&Operation>::with_capacity(ops.len());

    for op in ops.drain(..) {
        let mut index = 0;
        while index < sorted.len() && rga_sibling_after(op, sorted[index]) {
            index += 1;
        }
        sorted.insert(index, op);
    }

    *ops = sorted;
}

fn rga_sibling_after(left: &Operation, right: &Operation) -> bool {
    if left.origin_right == Some(right.id) {
        return false;
    }
    if right.origin_right == Some(left.id) {
        return true;
    }
    left.id > right.id
}

fn is_stable(id: OpId, stable: &StateVector) -> bool {
    stable.clock_for(id.client) >= id.clock
}

impl TextDocument {
    fn deleted_targets(&self) -> BTreeMap<OpId, OpId> {
        self.doc
            .store
            .values()
            .filter_map(|op| match op.content {
                OpContent::Delete => op.origin_left.map(|target| (target, op.id)),
                _ => None,
            })
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TextItem {
    id: OpId,
    origin_left: Option<OpId>,
    origin_right: Option<OpId>,
    text: String,
    visible: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CrdtError {
    InvalidClientId,
    IndexOutOfBounds,
    UnexpectedEof,
    InvalidContentType(u8),
    ContentTooLarge,
    InvalidUtf8,
    TrailingBytes,
}

impl fmt::Display for CrdtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidClientId => write!(f, "client id must be non-zero"),
            Self::IndexOutOfBounds => write!(f, "text index is out of bounds"),
            Self::UnexpectedEof => write!(f, "encoded operation ended unexpectedly"),
            Self::InvalidContentType(value) => write!(f, "invalid operation content type: {value}"),
            Self::ContentTooLarge => write!(f, "operation content is too large"),
            Self::InvalidUtf8 => write!(f, "text operation content is not valid UTF-8"),
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
            1 => OpContent::Text(
                String::from_utf8(payload.to_vec()).map_err(|_| CrdtError::InvalidUtf8)?,
            ),
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

pub fn encode_state_vector(vector: &StateVector) -> Result<Vec<u8>, CrdtError> {
    let entries = vector.entries().collect::<Vec<_>>();
    let mut bytes = Vec::with_capacity(4 + entries.len() * 16);
    bytes.extend_from_slice(&(entries.len() as u32).to_be_bytes());

    for (client, clock) in entries {
        if client == 0 {
            return Err(CrdtError::InvalidClientId);
        }
        bytes.extend_from_slice(&client.to_be_bytes());
        bytes.extend_from_slice(&clock.to_be_bytes());
    }

    Ok(bytes)
}

pub fn decode_state_vector(bytes: &[u8]) -> Result<StateVector, CrdtError> {
    let mut cursor = Cursor::new(bytes);
    let count = cursor.u32()? as usize;
    let mut entries = Vec::with_capacity(count);

    for _ in 0..count {
        let client = cursor.u64()?;
        let clock = cursor.u64()?;
        entries.push((client, clock));
    }

    if !cursor.is_done() {
        return Err(CrdtError::TrailingBytes);
    }

    StateVector::from_entries(entries)
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
    use proptest::prelude::*;

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
    fn encodes_and_decodes_state_vectors() {
        let vector = StateVector::from_entries([(1, 3), (2, 7)]).unwrap();

        let decoded = decode_state_vector(&encode_state_vector(&vector).unwrap()).unwrap();

        assert_eq!(decoded.clock_for(1), 3);
        assert_eq!(decoded.clock_for(2), 7);
    }

    #[test]
    fn diffs_from_encoded_state_vector() {
        let mut alice = TextDocument::new("doc", 1).unwrap();
        let mut bob = TextDocument::new("doc", 2).unwrap();

        bob.apply_update(&alice.insert(0, "Ra").unwrap()).unwrap();
        let missing = alice
            .diff_from_encoded_state_vector(&bob.encode_state_vector().unwrap())
            .unwrap();
        assert!(decode_operations(&missing).unwrap().is_empty());

        let new_update = alice.insert(2, "ft").unwrap();
        let diff = alice
            .diff_from_encoded_state_vector(&bob.encode_state_vector().unwrap())
            .unwrap();

        assert_eq!(
            decode_operations(&diff).unwrap(),
            decode_operations(&new_update).unwrap()
        );
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

    #[test]
    fn drains_chained_pending_operations() {
        let first = Operation::text(OpId::new(1, 1).unwrap(), "a");
        let mut second = Operation::text(OpId::new(1, 2).unwrap(), "b");
        let mut third = Operation::text(OpId::new(1, 3).unwrap(), "c");
        second.origin_left = Some(first.id);
        third.origin_left = Some(second.id);

        let mut doc = Document::new("doc");
        doc.integrate_operation(third).unwrap();
        doc.integrate_operation(second).unwrap();
        doc.integrate_operation(first).unwrap();

        assert_eq!(doc.operations().count(), 3);
        assert_eq!(doc.state_vector().clock_for(1), 3);
    }

    #[test]
    fn text_document_inserts_and_deletes_text() {
        let mut doc = TextDocument::new("doc", 1).unwrap();

        doc.insert(0, "raft").unwrap();
        doc.insert(2, "ft").unwrap();
        doc.delete(2, 2).unwrap();

        assert_eq!(doc.text(), "raft");
    }

    #[test]
    fn text_documents_converge_with_binary_updates() {
        let mut alice = TextDocument::new("doc", 1).unwrap();
        let mut bob = TextDocument::new("doc", 2).unwrap();

        let a1 = alice.insert(0, "Ra").unwrap();
        bob.apply_update(&a1).unwrap();

        let b1 = bob.insert(2, "ft").unwrap();
        alice.apply_update(&b1).unwrap();

        let a2 = alice.delete(1, 1).unwrap();
        bob.apply_update(&a2).unwrap();

        assert_eq!(alice.text(), "Rft");
        assert_eq!(bob.text(), alice.text());
    }

    #[test]
    fn text_document_restores_from_encoded_state() {
        let mut doc = TextDocument::new("doc", 1).unwrap();
        doc.insert(0, "hello").unwrap();
        doc.delete(1, 1).unwrap();

        let state = doc.encode_state().unwrap();
        let mut restored = TextDocument::new("doc", 2).unwrap();
        restored.apply_update(&state).unwrap();

        assert_eq!(restored.text(), "hllo");
    }

    #[test]
    fn rga_orders_concurrent_siblings_deterministically() {
        let mut left = TextDocument::new("doc", 1).unwrap();
        let mut right = TextDocument::new("doc", 2).unwrap();

        let seed = left.insert(0, "a").unwrap();
        right.apply_update(&seed).unwrap();

        let left_update = left.insert(1, "x").unwrap();
        let right_update = right.insert(1, "y").unwrap();

        left.apply_update(&right_update).unwrap();
        right.apply_update(&left_update).unwrap();

        assert_eq!(left.text(), right.text());
        assert_eq!(left.document().pending_len(), 0);
        assert_eq!(right.document().pending_len(), 0);
    }

    #[test]
    fn tombstone_compaction_removes_stable_unreferenced_deletes() {
        let mut doc = TextDocument::new("doc", 1).unwrap();
        doc.insert(0, "ab").unwrap();
        doc.delete(1, 1).unwrap();

        let before = doc.document().operations().count();
        let stable = StateVector::from_entries([(1, 3)]).unwrap();
        let removed = doc.compact_tombstones(&stable);

        assert_eq!(removed, 2);
        assert_eq!(doc.text(), "a");
        assert_eq!(doc.document().operations().count(), before - 2);
    }

    #[test]
    fn tombstone_compaction_keeps_referenced_deleted_anchors() {
        let mut doc = TextDocument::new("doc", 1).unwrap();
        doc.insert(0, "abc").unwrap();
        doc.delete(1, 1).unwrap();

        let stable = StateVector::from_entries([(1, 4)]).unwrap();
        let removed = doc.compact_tombstones(&stable);

        assert_eq!(removed, 0);
        assert_eq!(doc.text(), "ac");
    }

    proptest! {
        #[test]
        fn randomized_replay_converges(actions in prop::collection::vec(any::<ModelAction>(), 1..80)) {
            let updates = build_random_updates(&actions);
            let mut forward = TextDocument::new("doc", 101).unwrap();
            let mut reverse = TextDocument::new("doc", 102).unwrap();
            let mut shuffled = TextDocument::new("doc", 103).unwrap();

            for update in &updates {
                forward.apply_update(update).unwrap();
            }
            for update in updates.iter().rev() {
                reverse.apply_update(update).unwrap();
            }
            for index in deterministic_shuffle_indices(updates.len()) {
                shuffled.apply_update(&updates[index]).unwrap();
            }

            prop_assert_eq!(forward.text(), reverse.text());
            prop_assert_eq!(forward.text(), shuffled.text());
            prop_assert_eq!(forward.document().pending_len(), 0);
            prop_assert_eq!(reverse.document().pending_len(), 0);
            prop_assert_eq!(shuffled.document().pending_len(), 0);
        }
    }

    #[derive(Clone, Debug)]
    enum ModelAction {
        Insert {
            client: u64,
            index_hint: usize,
            ch: char,
        },
        Delete {
            client: u64,
            index_hint: usize,
        },
    }

    impl Arbitrary for ModelAction {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                (1_u64..=3, 0_usize..24, 0_u8..26).prop_map(|(client, index_hint, ch)| {
                    Self::Insert {
                        client,
                        index_hint,
                        ch: char::from(b'a' + ch),
                    }
                }),
                (1_u64..=3, 0_usize..24)
                    .prop_map(|(client, index_hint)| Self::Delete { client, index_hint }),
            ]
            .boxed()
        }
    }

    fn build_random_updates(actions: &[ModelAction]) -> Vec<Vec<u8>> {
        let mut docs = BTreeMap::<ClientId, TextDocument>::new();
        let mut updates = Vec::new();

        for action in actions {
            match *action {
                ModelAction::Insert {
                    client,
                    index_hint,
                    ch,
                } => {
                    let doc = docs
                        .entry(client)
                        .or_insert_with(|| TextDocument::new("doc", client).unwrap());
                    let index = bounded_index(index_hint, doc.text().chars().count(), true);
                    let update = doc.insert(index, &ch.to_string()).unwrap();
                    apply_to_other_docs(client, &mut docs, &update);
                    updates.push(update);
                }
                ModelAction::Delete { client, index_hint } => {
                    let doc = docs
                        .entry(client)
                        .or_insert_with(|| TextDocument::new("doc", client).unwrap());
                    let len = doc.text().chars().count();
                    if len == 0 {
                        continue;
                    }
                    let index = bounded_index(index_hint, len, false);
                    let update = doc.delete(index, 1).unwrap();
                    apply_to_other_docs(client, &mut docs, &update);
                    updates.push(update);
                }
            }
        }

        updates
    }

    fn apply_to_other_docs(
        client: ClientId,
        docs: &mut BTreeMap<ClientId, TextDocument>,
        update: &[u8],
    ) {
        for (id, doc) in docs {
            if *id != client {
                doc.apply_update(update).unwrap();
            }
        }
    }

    fn bounded_index(index_hint: usize, len: usize, allow_end: bool) -> usize {
        let modulus = if allow_end { len + 1 } else { len };
        if modulus == 0 {
            0
        } else {
            index_hint % modulus
        }
    }

    fn deterministic_shuffle_indices(len: usize) -> Vec<usize> {
        let mut remaining = (0..len).collect::<BTreeSet<_>>();
        let mut order = Vec::with_capacity(len);
        let mut cursor = 0usize;

        while !remaining.is_empty() {
            cursor = (cursor + 7) % remaining.len();
            let index = *remaining.iter().nth(cursor).unwrap();
            remaining.remove(&index);
            order.push(index);
        }

        order
    }
}
