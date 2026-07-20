use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use rhiza_core::{LogHash, LogIndex};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

pub const QWAL_V2_MAGIC: &[u8; 6] = b"QWAL\0\x03";
pub const MAX_QWAL_V2_BYTES: usize = 512 * 1024;
pub const MAX_QWAL_V2_RECEIPTS: usize = 1024;

const SQLITE_HEADER_BYTES: usize = 100;
const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
const MIN_SQLITE_PAGE_SIZE: u32 = 512;
const MAX_SQLITE_PAGE_SIZE: u32 = 65_536;
const MAX_ID_BYTES: usize = 256;
const MAX_FINGERPRINT_BYTES: usize = 4 * 1024;

/// A final page image in a closed SQLite database.
///
/// Page numbers are one-based, as they are in SQLite's file format.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QwalPageV2 {
    pub page_no: u64,
    pub after_image: Vec<u8>,
}

/// One successful member of a QWAL v2 batch.
///
/// The decided entry anchor is intentionally absent: every receipt in the
/// envelope is installed at the single anchor of the log entry carrying the
/// batch.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QwalReceiptV2 {
    pub request_id: String,
    pub request_digest: LogHash,
    pub result_blob: Vec<u8>,
}

/// Canonical QWAL v2 page effect decided by the replicated log.
///
/// The envelope deliberately contains no local path or WAL-index state. The
/// caller owns the control-sidecar intent and atomic installation protocol.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QwalEnvelopeV2 {
    pub cluster_id: String,
    pub epoch: u64,
    pub configuration_id: u64,
    pub recovery_generation: u64,
    pub base_index: LogIndex,
    pub base_hash: LogHash,
    pub base_db_digest: LogHash,
    pub base_file_bytes: u64,
    pub target_db_digest: LogHash,
    pub target_file_bytes: u64,
    pub materializer_fingerprint: String,
    pub page_size: u32,
    pub receipts: Vec<QwalReceiptV2>,
    pub pages: Vec<QwalPageV2>,
}

impl QwalEnvelopeV2 {
    /// Validates every structural property that does not require the base file.
    pub fn validate(&self) -> Result<()> {
        validate_nonempty_bounded("cluster_id", &self.cluster_id, MAX_ID_BYTES)?;
        validate_nonempty_bounded(
            "materializer_fingerprint",
            &self.materializer_fingerprint,
            MAX_FINGERPRINT_BYTES,
        )?;
        validate_page_size(self.page_size)?;

        if self.receipts.is_empty() || self.receipts.len() > MAX_QWAL_V2_RECEIPTS {
            return invalid(format!(
                "QWAL receipts must contain 1..={MAX_QWAL_V2_RECEIPTS} members"
            ));
        }
        let mut result_bytes = 0usize;
        let mut request_ids = HashSet::with_capacity(self.receipts.len());
        for receipt in &self.receipts {
            validate_nonempty_bounded("request_id", &receipt.request_id, MAX_ID_BYTES)?;
            if !request_ids.insert(receipt.request_id.as_str()) {
                return invalid("QWAL receipt request ids must be unique");
            }
            crate::validate_sql_result_blob_bounds(&receipt.result_blob)
                .map_err(|_| Error::InvalidEntry("QWAL receipt result is not canonical".into()))?;
            result_bytes = result_bytes
                .checked_add(receipt.result_blob.len())
                .ok_or_else(|| Error::ResourceExhausted("QWAL result bytes overflow".into()))?;
            if result_bytes > MAX_QWAL_V2_BYTES {
                return Err(Error::ResourceExhausted(format!(
                    "QWAL results exceed {MAX_QWAL_V2_BYTES} bytes"
                )));
            }
        }

        let page_size = u64::from(self.page_size);
        if self.base_file_bytes == 0 || !self.base_file_bytes.is_multiple_of(page_size) {
            return invalid("base file size must be a non-zero page-size multiple");
        }
        if self.target_file_bytes == 0 || !self.target_file_bytes.is_multiple_of(page_size) {
            return invalid("target file size must be a non-zero page-size multiple");
        }

        let mut previous = 0;
        let mut page_bytes = 0usize;
        for page in &self.pages {
            if page.page_no == 0 {
                return invalid("QWAL page number must be one-based");
            }
            if page.page_no <= previous {
                return invalid("QWAL pages must be strictly ordered without duplicates");
            }
            if page.after_image.len() != self.page_size as usize {
                return invalid("QWAL after-image length does not match page size");
            }
            let page_end = page
                .page_no
                .checked_mul(page_size)
                .ok_or_else(|| Error::InvalidEntry("QWAL page offset overflows".into()))?;
            if page_end > self.target_file_bytes {
                return invalid("QWAL page lies outside the target file");
            }
            if page.page_no == 1 {
                validate_sqlite_header(&page.after_image, self.page_size)?;
                validate_header_page_count(&page.after_image, self.target_file_bytes, page_size)?;
            }
            page_bytes = page_bytes
                .checked_add(page.after_image.len())
                .ok_or_else(|| Error::ResourceExhausted("QWAL page bytes overflow".into()))?;
            if page_bytes > MAX_QWAL_V2_BYTES {
                return Err(Error::ResourceExhausted(format!(
                    "QWAL page images exceed {MAX_QWAL_V2_BYTES} bytes"
                )));
            }
            previous = page.page_no;
        }

        // Growing via set_len alone would create an attacker-sized sparse
        // file. Every page beyond the base EOF must therefore be carried by
        // the bounded envelope. Since pages are already strictly ordered and
        // range checked, cardinality proves that the suffix is gap-free.
        let base_pages = self.base_file_bytes / page_size;
        let target_pages = self.target_file_bytes / page_size;
        if target_pages > base_pages {
            let first_new = self
                .pages
                .partition_point(|page| page.page_no <= base_pages);
            let supplied_new_pages = u64::try_from(self.pages.len() - first_new)
                .map_err(|_| Error::ResourceExhausted("QWAL page count overflows".into()))?;
            let required_new_pages = target_pages - base_pages;
            if supplied_new_pages != required_new_pages {
                return invalid("QWAL growth must include every newly allocated page");
            }
        }

        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_qwal_v2(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode_qwal_v2(bytes)
    }
}

pub fn encode_qwal_v2(effect: &QwalEnvelopeV2) -> Result<Vec<u8>> {
    effect.validate()?;
    let body = postcard::to_allocvec(effect)
        .map_err(|error| Error::InvalidEntry(format!("QWAL encode failed: {error}")))?;
    let encoded_len = QWAL_V2_MAGIC
        .len()
        .checked_add(body.len())
        .ok_or_else(|| Error::ResourceExhausted("QWAL encoded length overflows".into()))?;
    if encoded_len > MAX_QWAL_V2_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "QWAL envelope exceeds {MAX_QWAL_V2_BYTES} bytes"
        )));
    }
    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.extend_from_slice(QWAL_V2_MAGIC);
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

pub fn decode_qwal_v2(bytes: &[u8]) -> Result<QwalEnvelopeV2> {
    if bytes.len() > MAX_QWAL_V2_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "QWAL envelope exceeds {MAX_QWAL_V2_BYTES} bytes"
        )));
    }
    let Some(body) = bytes.strip_prefix(QWAL_V2_MAGIC) else {
        return invalid("invalid QWAL v2 magic");
    };
    if body.is_empty() {
        return invalid("empty QWAL v2 body");
    }
    let effect: QwalEnvelopeV2 = postcard::from_bytes(body)
        .map_err(|error| Error::InvalidEntry(format!("QWAL decode failed: {error}")))?;
    effect.validate()?;

    // Reject alternate integer encodings and any trailing bytes. This also
    // makes the qlog payload a unique byte representation of the envelope.
    let canonical = postcard::to_allocvec(&effect)
        .map_err(|error| Error::InvalidEntry(format!("QWAL re-encode failed: {error}")))?;
    if canonical.as_slice() != body {
        return invalid("QWAL body is not canonically encoded");
    }
    Ok(effect)
}

/// Computes SHA-256 without reading the entire database into memory.
pub fn file_digest(path: impl AsRef<Path>) -> Result<LogHash> {
    let mut file = File::open(path.as_ref()).map_err(io_error)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(io_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(LogHash::from_bytes(hasher.finalize().into()))
}

/// Reads and validates the page size in a closed SQLite database header.
pub fn sqlite_page_size(path: impl AsRef<Path>) -> Result<u32> {
    let mut file = File::open(path.as_ref()).map_err(io_error)?;
    let mut header = [0u8; SQLITE_HEADER_BYTES];
    file.read_exact(&mut header).map_err(io_error)?;
    let page_size = page_size_from_header(&header)?;
    let file_bytes = file.metadata().map_err(io_error)?.len();
    if file_bytes == 0 || file_bytes % u64::from(page_size) != 0 {
        return invalid("SQLite file size is not a non-zero page-size multiple");
    }
    validate_header_page_count(&header, file_bytes, u64::from(page_size))?;
    Ok(page_size)
}

/// Produces sorted final page images by comparing two closed SQLite files.
///
/// Pages removed by a shrink are represented only by `target_file_bytes` in
/// the envelope; this function therefore returns images for target pages only.
/// The target digest is computed from the same complete target scan.
pub fn diff_closed_databases(
    base_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
) -> Result<(Vec<QwalPageV2>, LogHash)> {
    let base_path = base_path.as_ref();
    let target_path = target_path.as_ref();
    let base_page_size = sqlite_page_size(base_path)?;
    let target_page_size = sqlite_page_size(target_path)?;
    if base_page_size != target_page_size {
        return invalid("SQLite base and target page sizes differ");
    }

    let page_size = base_page_size as usize;
    let mut base = File::open(base_path).map_err(io_error)?;
    let mut target = File::open(target_path).map_err(io_error)?;
    let target_bytes = target.metadata().map_err(io_error)?.len();
    let target_pages = target_bytes / u64::from(base_page_size);
    let mut base_page = vec![0; page_size];
    let mut target_page = vec![0; page_size];
    let mut target_hasher = Sha256::new();
    let mut pages = Vec::new();
    let mut changed_bytes = 0usize;

    for page_index in 0..target_pages {
        target.read_exact(&mut target_page).map_err(io_error)?;
        target_hasher.update(&target_page);
        let base_has_page = read_page_or_eof(&mut base, &mut base_page)?;
        if !base_has_page || base_page != target_page {
            changed_bytes = changed_bytes
                .checked_add(page_size)
                .ok_or_else(|| Error::ResourceExhausted("QWAL diff size overflows".into()))?;
            if changed_bytes > MAX_QWAL_V2_BYTES {
                return Err(Error::ResourceExhausted(format!(
                    "QWAL changed pages exceed {MAX_QWAL_V2_BYTES} bytes"
                )));
            }
            pages.push(QwalPageV2 {
                page_no: page_index + 1,
                after_image: target_page.clone(),
            });
        }
    }
    Ok((pages, LogHash::from_bytes(target_hasher.finalize().into())))
}

/// Copies `base_path` to a new temp path and applies a validated page effect.
///
/// `temp_path` must not already exist. On any error, a partially written temp
/// file is removed. The authoritative database is never modified here.
pub fn apply_qwal_to_file(
    base_path: impl AsRef<Path>,
    temp_path: impl AsRef<Path>,
    effect: &QwalEnvelopeV2,
) -> Result<()> {
    effect.validate()?;
    let base_path = base_path.as_ref();
    let temp_path = temp_path.as_ref();
    if base_path == temp_path {
        return invalid("QWAL temp path must differ from the base path");
    }

    let base_bytes = fs::metadata(base_path).map_err(io_error)?.len();
    if base_bytes != effect.base_file_bytes {
        return invalid("QWAL base file size mismatch");
    }
    if sqlite_page_size(base_path)? != effect.page_size {
        return invalid("QWAL base page size mismatch");
    }
    if file_digest(base_path)? != effect.base_db_digest {
        return invalid("QWAL base database digest mismatch");
    }

    let temp = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(temp_path)
        .map_err(io_error)?;
    let outcome = apply_to_new_temp(base_path, temp_path, temp, effect);
    if outcome.is_err() {
        // This path is removed only after this invocation successfully
        // created it. A pre-existing file or symlink is never unlinked when
        // CREATE_NEW rejects it.
        let _ = fs::remove_file(temp_path);
    }
    outcome
}

fn apply_to_new_temp(
    base_path: &Path,
    temp_path: &Path,
    mut temp: File,
    effect: &QwalEnvelopeV2,
) -> Result<()> {
    let mut base = File::open(base_path).map_err(io_error)?;
    std::io::copy(&mut base, &mut temp).map_err(io_error)?;
    temp.set_len(effect.target_file_bytes).map_err(io_error)?;

    let page_size = u64::from(effect.page_size);
    for page in &effect.pages {
        let offset = (page.page_no - 1)
            .checked_mul(page_size)
            .ok_or_else(|| Error::InvalidEntry("QWAL page offset overflows".into()))?;
        temp.seek(SeekFrom::Start(offset)).map_err(io_error)?;
        temp.write_all(&page.after_image).map_err(io_error)?;
    }
    drop(temp);

    if file_digest(temp_path)? != effect.target_db_digest {
        return invalid("QWAL target database digest mismatch");
    }
    if sqlite_page_size(temp_path)? != effect.page_size {
        return invalid("QWAL target page size mismatch");
    }
    Ok(())
}

fn read_page_or_eof(file: &mut File, page: &mut [u8]) -> Result<bool> {
    let mut read = 0;
    while read < page.len() {
        let count = file.read(&mut page[read..]).map_err(io_error)?;
        if count == 0 {
            if read == 0 {
                return Ok(false);
            }
            return invalid("SQLite base file ends in a partial page");
        }
        read += count;
    }
    Ok(true)
}

fn page_size_from_header(header: &[u8]) -> Result<u32> {
    if header.len() < SQLITE_HEADER_BYTES || &header[..SQLITE_MAGIC.len()] != SQLITE_MAGIC {
        return invalid("invalid SQLite database header");
    }
    let encoded = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if encoded == 1 {
        MAX_SQLITE_PAGE_SIZE
    } else {
        u32::from(encoded)
    };
    validate_page_size(page_size)?;
    Ok(page_size)
}

fn validate_page_size(page_size: u32) -> Result<()> {
    if !(MIN_SQLITE_PAGE_SIZE..=MAX_SQLITE_PAGE_SIZE).contains(&page_size)
        || !page_size.is_power_of_two()
    {
        return invalid("invalid SQLite page size");
    }
    Ok(())
}

fn validate_sqlite_header(page: &[u8], expected_page_size: u32) -> Result<()> {
    let actual = page_size_from_header(page)?;
    if actual != expected_page_size {
        return invalid("QWAL page 1 changes the declared SQLite page size");
    }
    Ok(())
}

fn validate_header_page_count(header: &[u8], file_bytes: u64, page_size: u64) -> Result<()> {
    let declared = u32::from_be_bytes([header[28], header[29], header[30], header[31]]);
    // A zero page count is permitted by legacy SQLite files. When present, the
    // count must describe the closed file exactly.
    if declared != 0 && u64::from(declared) != file_bytes / page_size {
        return invalid("SQLite header page count does not match file size");
    }
    Ok(())
}

fn validate_nonempty_bounded(field: &str, value: &str, max: usize) -> Result<()> {
    if value.is_empty() || value.len() > max {
        return invalid(format!("QWAL {field} must contain 1..={max} bytes"));
    }
    Ok(())
}

fn io_error(error: std::io::Error) -> Error {
    Error::Io(error.to_string())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::InvalidEntry(message.into()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rusqlite::{params, Connection};

    use super::*;

    #[test]
    fn generation_capacity_is_1024_receipts_and_512_kib() {
        assert_eq!(MAX_QWAL_V2_RECEIPTS, 1024);
        assert_eq!(MAX_QWAL_V2_BYTES, 512 * 1024);
    }

    fn sqlite_header(page_size: u32, page_count: u32) -> Vec<u8> {
        let mut page = vec![0; page_size as usize];
        page[..SQLITE_MAGIC.len()].copy_from_slice(SQLITE_MAGIC);
        let encoded = if page_size == MAX_SQLITE_PAGE_SIZE {
            1u16
        } else {
            page_size as u16
        };
        page[16..18].copy_from_slice(&encoded.to_be_bytes());
        page[18] = 1;
        page[19] = 1;
        page[20] = 0;
        page[21] = 64;
        page[22] = 32;
        page[23] = 32;
        page[28..32].copy_from_slice(&page_count.to_be_bytes());
        page
    }

    fn write_pages(path: &Path, page_size: u32, fills: &[u8]) {
        let mut bytes = Vec::with_capacity(page_size as usize * fills.len());
        for (index, fill) in fills.iter().enumerate() {
            let mut page = vec![*fill; page_size as usize];
            if index == 0 {
                page = sqlite_header(page_size, fills.len() as u32);
                page[100..].fill(*fill);
            }
            bytes.extend_from_slice(&page);
        }
        fs::write(path, bytes).unwrap();
    }

    fn envelope(base: &Path, target: &Path, pages: Vec<QwalPageV2>) -> QwalEnvelopeV2 {
        QwalEnvelopeV2 {
            cluster_id: "cluster-a".into(),
            epoch: 3,
            configuration_id: 4,
            recovery_generation: 5,
            base_index: 8,
            base_hash: LogHash::digest(&[b"base-anchor"]),
            base_db_digest: file_digest(base).unwrap(),
            base_file_bytes: fs::metadata(base).unwrap().len(),
            target_db_digest: file_digest(target).unwrap(),
            target_file_bytes: fs::metadata(target).unwrap().len(),
            materializer_fingerprint: "sqlite-test-qwal-v2".into(),
            page_size: sqlite_page_size(base).unwrap(),
            receipts: vec![QwalReceiptV2 {
                request_id: "request-a".into(),
                request_digest: LogHash::digest(&[b"request"]),
                result_blob: crate::encode_sql_result(&crate::SqlCommandResult {
                    statement_results: Vec::new(),
                })
                .unwrap(),
            }],
            pages,
        }
    }

    fn diff_pages(base: &Path, target: &Path) -> Vec<QwalPageV2> {
        diff_closed_databases(base, target).unwrap().0
    }

    #[test]
    fn qwal_roundtrips_through_its_only_canonical_encoding() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        write_pages(&base, 512, &[1, 2]);
        write_pages(&target, 512, &[1, 3]);
        let effect = envelope(&base, &target, diff_pages(&base, &target));

        let encoded = effect.encode().unwrap();
        assert!(encoded.starts_with(QWAL_V2_MAGIC));
        assert_eq!(QwalEnvelopeV2::decode(&encoded).unwrap(), effect);
    }

    #[test]
    fn closed_file_diff_returns_the_exact_target_digest_for_every_shape() {
        let large_base = vec![1; 1_024];
        let mut large_target = large_base.clone();
        large_target[1_023] = 2;
        for (base_fills, target_fills) in [
            (vec![1], vec![2, 3, 4]),
            (vec![1, 2, 3], vec![4]),
            (vec![1, 2, 3], vec![1, 2, 3]),
            (large_base, large_target),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let base = dir.path().join("base.db");
            let target = dir.path().join("target.db");
            write_pages(&base, 512, &base_fills);
            write_pages(&target, 512, &target_fills);

            let (_, target_digest) = diff_closed_databases(&base, &target).unwrap();

            assert_eq!(target_digest, file_digest(&target).unwrap());
        }
    }

    #[test]
    fn fused_target_digest_preserves_canonical_qwal_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        write_pages(&base, 512, &[1, 2]);
        write_pages(&target, 512, &[1, 3]);
        let (pages, target_digest) = diff_closed_databases(&base, &target).unwrap();
        let legacy = envelope(&base, &target, pages.clone());
        let mut fused = envelope(&base, &target, pages);
        fused.target_db_digest = target_digest;

        assert_eq!(fused.encode().unwrap(), legacy.encode().unwrap());
    }

    #[test]
    fn decoder_rejects_trailing_and_corrupted_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        write_pages(&base, 512, &[1]);
        write_pages(&target, 512, &[2]);
        let effect = envelope(&base, &target, diff_pages(&base, &target));
        let mut trailing = effect.encode().unwrap();
        trailing.push(0);
        assert!(QwalEnvelopeV2::decode(&trailing).is_err());

        let mut corrupt = effect.encode().unwrap();
        corrupt[0] ^= 0xff;
        assert!(QwalEnvelopeV2::decode(&corrupt).is_err());

        for old_magic in [b"QWAL\0\x01", b"QWAL\0\x02"] {
            let mut old = old_magic.to_vec();
            old.extend_from_slice(&effect.encode().unwrap()[QWAL_V2_MAGIC.len()..]);
            assert!(QwalEnvelopeV2::decode(&old).is_err());
        }
    }

    #[test]
    fn receipts_preserve_order_and_reject_empty_duplicate_or_oversized_batches() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        write_pages(&base, 512, &[1]);
        let mut effect = envelope(&base, &base, Vec::new());
        effect.receipts = (0..MAX_QWAL_V2_RECEIPTS)
            .map(|index| QwalReceiptV2 {
                request_id: format!("request-{index:03}"),
                request_digest: LogHash::digest(&[&index.to_le_bytes()]),
                result_blob: crate::encode_sql_result(&crate::SqlCommandResult {
                    statement_results: Vec::new(),
                })
                .unwrap(),
            })
            .collect();
        let decoded = QwalEnvelopeV2::decode(&effect.encode().unwrap()).unwrap();
        assert_eq!(decoded.receipts, effect.receipts);

        effect.receipts[MAX_QWAL_V2_RECEIPTS - 1].request_id =
            effect.receipts[0].request_id.clone();
        assert!(matches!(
            effect.validate(),
            Err(Error::InvalidEntry(message)) if message.contains("unique")
        ));

        effect.receipts.clear();
        assert!(effect.validate().is_err());

        effect.receipts = vec![
            QwalReceiptV2 {
                request_id: "duplicate".into(),
                request_digest: LogHash::ZERO,
                result_blob: crate::encode_sql_result(&crate::SqlCommandResult {
                    statement_results: Vec::new(),
                })
                .unwrap(),
            },
            QwalReceiptV2 {
                request_id: "duplicate".into(),
                request_digest: LogHash::digest(&[b"different"]),
                result_blob: crate::encode_sql_result(&crate::SqlCommandResult {
                    statement_results: Vec::new(),
                })
                .unwrap(),
            },
        ];
        assert!(effect.validate().is_err());

        effect.receipts = (0..=MAX_QWAL_V2_RECEIPTS)
            .map(|index| QwalReceiptV2 {
                request_id: format!("request-{index}"),
                request_digest: LogHash::ZERO,
                result_blob: crate::encode_sql_result(&crate::SqlCommandResult {
                    statement_results: Vec::new(),
                })
                .unwrap(),
            })
            .collect();
        assert!(effect.validate().is_err());
    }

    #[test]
    fn validation_rejects_zero_duplicate_unordered_short_and_out_of_range_pages() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        write_pages(&base, 512, &[1, 2]);
        write_pages(&target, 512, &[2, 3]);
        let page = diff_pages(&base, &target)[0].clone();

        for pages in [
            vec![QwalPageV2 {
                page_no: 0,
                after_image: vec![0; 512],
            }],
            vec![page.clone(), page.clone()],
            vec![
                QwalPageV2 {
                    page_no: 2,
                    after_image: vec![0; 512],
                },
                page.clone(),
            ],
            vec![QwalPageV2 {
                page_no: 1,
                after_image: vec![0; 511],
            }],
            vec![QwalPageV2 {
                page_no: 3,
                after_image: vec![0; 512],
            }],
        ] {
            assert!(envelope(&base, &target, pages).validate().is_err());
        }
    }

    #[test]
    fn page_one_must_remain_a_valid_sqlite_header() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        write_pages(&base, 512, &[1]);
        write_pages(&target, 512, &[2]);
        let mut effect = envelope(&base, &target, diff_pages(&base, &target));
        effect.pages[0].after_image[0] = b'X';
        assert!(effect.validate().is_err());
    }

    #[test]
    fn closed_file_diff_and_apply_support_growth_and_shrink() {
        for (base_fills, target_fills) in [
            (vec![1], vec![2, 3, 4]),
            (vec![1, 2, 3], vec![4]),
            (vec![1, 2, 3], vec![1, 8, 3]),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let base = dir.path().join("base.db");
            let target = dir.path().join("target.db");
            let applied = dir.path().join("applied.db");
            write_pages(&base, 512, &base_fills);
            write_pages(&target, 512, &target_fills);
            let pages = diff_pages(&base, &target);
            let effect = envelope(&base, &target, pages);

            apply_qwal_to_file(&base, &applied, &effect).unwrap();
            assert_eq!(fs::read(&applied).unwrap(), fs::read(&target).unwrap());
        }
    }

    #[test]
    fn validation_rejects_sparse_growth_and_missing_new_pages_before_file_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        write_pages(&base, 512, &[1]);
        write_pages(&target, 512, &[2, 3, 4]);

        let mut gap = envelope(&base, &target, diff_pages(&base, &target));
        gap.pages.retain(|page| page.page_no != 2);
        assert!(gap.validate().is_err());

        let mut sparse = envelope(&base, &base, Vec::new());
        sparse.target_file_bytes = u64::MAX - (u64::MAX % 512);
        sparse.target_db_digest = LogHash::ZERO;
        let absent = dir.path().join("must-not-be-created.db");
        assert!(sparse.validate().is_err());
        assert!(apply_qwal_to_file(&base, &absent, &sparse).is_err());
        assert!(!absent.exists());
    }

    #[test]
    fn shrink_needs_no_images_for_pages_removed_by_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        let applied = dir.path().join("applied.db");
        write_pages(&base, 512, &[1, 2, 3]);
        write_pages(&target, 512, &[1]);
        let effect = envelope(&base, &target, diff_pages(&base, &target));
        assert!(effect.pages.iter().all(|page| page.page_no == 1));
        effect.validate().unwrap();
        apply_qwal_to_file(&base, &applied, &effect).unwrap();
        assert_eq!(fs::read(applied).unwrap(), fs::read(target).unwrap());
    }

    #[test]
    fn page_size_parser_accepts_every_sqlite_page_size_and_rejects_invalid_values() {
        let dir = tempfile::tempdir().unwrap();
        for exponent in 9..=16 {
            let size = 1u32 << exponent;
            let path = dir.path().join(format!("{size}.db"));
            write_pages(&path, size, &[1]);
            assert_eq!(sqlite_page_size(path).unwrap(), size);
        }
        let invalid = dir.path().join("invalid.db");
        let mut page = sqlite_header(512, 1);
        page[16..18].copy_from_slice(&768u16.to_be_bytes());
        fs::write(&invalid, page).unwrap();
        assert!(sqlite_page_size(invalid).is_err());
    }

    #[test]
    fn apply_rejects_wrong_base_and_removes_partial_target() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        let wrong = dir.path().join("wrong.db");
        let applied = dir.path().join("applied.db");
        write_pages(&base, 512, &[1]);
        write_pages(&target, 512, &[2]);
        write_pages(&wrong, 512, &[3]);
        let effect = envelope(&base, &target, diff_pages(&base, &target));

        assert!(apply_qwal_to_file(&wrong, &applied, &effect).is_err());
        assert!(!applied.exists());

        let mut corrupt_effect = effect.clone();
        corrupt_effect.target_db_digest = LogHash::ZERO;
        assert!(apply_qwal_to_file(&base, &applied, &corrupt_effect).is_err());
        assert!(!applied.exists());
    }

    #[test]
    fn apply_does_not_replace_or_remove_a_preexisting_temp_path() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        let occupied = dir.path().join("occupied.db");
        write_pages(&base, 512, &[1]);
        write_pages(&target, 512, &[2]);
        fs::write(&occupied, b"owned by caller").unwrap();
        let effect = envelope(&base, &target, diff_pages(&base, &target));

        assert!(apply_qwal_to_file(&base, &occupied, &effect).is_err());
        assert_eq!(fs::read(&occupied).unwrap(), b"owned by caller");
    }

    #[cfg(unix)]
    #[test]
    fn apply_does_not_follow_or_remove_a_preexisting_temp_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        let victim = dir.path().join("victim");
        let link = dir.path().join("temp-link");
        write_pages(&base, 512, &[1]);
        write_pages(&target, 512, &[2]);
        fs::write(&victim, b"do not touch").unwrap();
        symlink(&victim, &link).unwrap();
        let effect = envelope(&base, &target, diff_pages(&base, &target));

        assert!(apply_qwal_to_file(&base, &link, &effect).is_err());
        assert_eq!(fs::read(&victim).unwrap(), b"do not touch");
        assert_eq!(fs::read_link(&link).unwrap(), victim);
    }

    #[test]
    fn captured_sqlite_effect_reproduces_native_features_byte_for_byte() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        let applied = dir.path().join("applied.db");
        {
            let connection = Connection::open(&base).unwrap();
            connection
                .execute_batch(
                    "PRAGMA page_size=4096;
                     PRAGMA journal_mode=DELETE;
                     PRAGMA foreign_keys=ON;
                     CREATE TABLE parent(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
                     CREATE TABLE child(
                       id INTEGER PRIMARY KEY,
                       parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE,
                       payload BLOB,
                       created_at TEXT DEFAULT CURRENT_TIMESTAMP
                     );
                     CREATE TABLE audit(message TEXT);
                     CREATE TRIGGER child_audit AFTER INSERT ON child BEGIN
                       INSERT INTO audit VALUES ('child:' || NEW.id);
                     END;",
                )
                .unwrap();
        }
        fs::copy(&base, &target).unwrap();
        {
            let mut connection = Connection::open(&target).unwrap();
            connection.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
            let transaction = connection.transaction().unwrap();
            let parent_id: i64 = transaction
                .query_row(
                    "INSERT INTO parent(name) VALUES ('native') RETURNING id",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let returned: (i64, Vec<u8>) = transaction
                .query_row(
                    "INSERT INTO child(id, parent_id, payload)
                     VALUES (7, ?1, randomblob(32)) RETURNING id, payload",
                    params![parent_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(returned.0, 7);
            assert_eq!(returned.1.len(), 32);
            transaction.commit().unwrap();
        }

        let pages = diff_pages(&base, &target);
        assert!(!pages.is_empty());
        let effect = envelope(&base, &target, pages);
        apply_qwal_to_file(&base, &applied, &effect).unwrap();

        assert_eq!(
            file_digest(&applied).unwrap(),
            file_digest(&target).unwrap()
        );
        assert_eq!(fs::read(&applied).unwrap(), fs::read(&target).unwrap());
        let inspection =
            Connection::open_with_flags(&applied, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        assert_eq!(
            inspection
                .query_row("SELECT message FROM audit", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "child:7"
        );
        assert_eq!(
            inspection
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }
}
