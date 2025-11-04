use std::{
    collections::HashMap,
    pin::Pin,
    result::Result as StdResult,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::Context;
use bytes::{BufMut, Bytes, BytesMut};
use dav_server::fs::{
    DavDirEntry, DavFile as FsDavFile, DavFileSystem, DavMetaData, FsError, FsFuture, FsResult,
    FsStream, OpenOptions, ReadDirMeta,
};
use futures_util::{FutureExt, Stream, StreamExt};
use percent_encoding::percent_decode_str;
use reqwest::{Method, header::RANGE};
use tokio::sync::{Mutex, RwLock};

use super::{
    Client, DirCache,
    error::{Error, Result},
    model::{Contents as DirEntry, FileEntry},
};

impl From<Error> for FsError {
    fn from(value: Error) -> Self {
        match value {
            Error::Io { source } => source.into(),
            Error::NotFound => FsError::NotFound,
            Error::PasswordRequired | Error::PasswordWrong => FsError::Forbidden,
            _ => FsError::GeneralFailure,
        }
    }
}

impl DavMetaData for DirEntry {
    fn len(&self) -> u64 {
        self.size()
    }

    fn is_dir(&self) -> bool {
        matches!(self, DirEntry::Folder(_))
    }

    fn modified(&self) -> FsResult<std::time::SystemTime> {
        Ok(UNIX_EPOCH + Duration::from_secs(self.modtime()))
    }

    fn created(&self) -> FsResult<std::time::SystemTime> {
        Ok(UNIX_EPOCH + Duration::from_secs(self.created()))
    }
}

pub struct BufferedStream<S> {
    stream: S,
    buffer: BytesMut,
}

impl<S> BufferedStream<S>
where
    S: Stream<Item = StdResult<Bytes, reqwest::Error>> + Unpin,
{
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            buffer: BytesMut::new(),
        }
    }

    pub async fn take_n_bytes(&mut self, n: usize) -> StdResult<Bytes, reqwest::Error> {
        let mut result = BytesMut::with_capacity(n);

        while result.len() < n {
            if !self.buffer.is_empty() {
                let need = n - result.len();
                if self.buffer.len() > need {
                    result.put(self.buffer.split_to(need));
                    break;
                } else {
                    result.put(self.buffer.split_to(self.buffer.len()));
                }
            }

            match self.stream.next().await {
                Some(Ok(chunk)) => {
                    self.buffer = BytesMut::from(&chunk[..]);
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }

        Ok(result.freeze())
    }
}

type StreamResult = StdResult<Bytes, reqwest::Error>;
type StreamType = Pin<Box<dyn Stream<Item = StreamResult> + Send>>;
type StreamBuffer = BufferedStream<StreamType>;

struct DavFile {
    fs: DavFs,
    position: u64,
    file: FileEntry,
    stream_buffer: Mutex<Option<StreamBuffer>>,
}

impl std::fmt::Debug for DavFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DavFile")
            .field("position", &self.position)
            .field("size", &self.file.size)
            .field("id", &self.file.id)
            .finish()
    }
}

impl DavFile {
    fn new(fs: DavFs, file: FileEntry) -> Self {
        Self {
            fs,
            position: 0,
            file,
            stream_buffer: Mutex::new(None),
        }
    }
}

impl FsDavFile for DavFile {
    fn read_bytes(&'_ mut self, count: usize) -> FsFuture<'_, bytes::Bytes> {
        async move {
            if count == 0 {
                return Ok(Bytes::new());
            }

            let client = &self.fs.client;
            let position = self.position;
            let url = self.file.link.clone();
            let range_header = format!("bytes={}-", position);

            if self.stream_buffer.lock().await.is_none() {
                let stream = client
                    .request_builder_for_download_stream(Method::GET, url, self.file.bypassed)
                    .await?
                    .header(RANGE, range_header)
                    .send()
                    .await
                    .map_err(Error::from)?
                    .bytes_stream();

                let boxed_stream: Pin<
                    Box<dyn Stream<Item = StdResult<Bytes, reqwest::Error>> + Send>,
                > = Box::pin(stream);
                let buffered_stream = BufferedStream::new(boxed_stream);

                *self.stream_buffer.lock().await = Some(buffered_stream);
            }

            let bytes = self
                .stream_buffer
                .get_mut()
                .as_mut()
                .unwrap()
                .take_n_bytes(count)
                .await
                .map_err(Error::from)?;

            self.position += bytes.len() as u64;
            Ok(bytes)
        }
        .boxed()
    }

    fn metadata(&mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        async move { Ok(Box::new(DirEntry::File(self.file.clone())) as Box<dyn DavMetaData>) }
            .boxed()
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> FsFuture<'_, u64> {
        async move {
            let new_pos = match pos {
                std::io::SeekFrom::Start(offset) => offset as i128,
                std::io::SeekFrom::End(offset) => self.file.size as i128 + offset as i128,
                std::io::SeekFrom::Current(offset) => self.position as i128 + offset as i128,
            };

            if new_pos < 0 || new_pos as u64 > self.file.size {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "seek position out of bounds",
                )
                .into());
            }

            self.position = new_pos as u64;
            Ok(self.position)
        }
        .boxed()
    }

    fn write_buf(&'_ mut self, _buf: Box<dyn bytes::Buf + Send>) -> FsFuture<'_, ()> {
        async { Err(FsError::NotImplemented) }.boxed()
    }

    fn write_bytes(&'_ mut self, _buf: bytes::Bytes) -> FsFuture<'_, ()> {
        async { Err(FsError::NotImplemented) }.boxed()
    }

    fn flush(&'_ mut self) -> FsFuture<'_, ()> {
        async { Err(FsError::NotImplemented) }.boxed()
    }
}

impl DavDirEntry for DirEntry {
    fn name(&self) -> Vec<u8> {
        self.name().as_bytes().to_vec()
    }

    fn metadata(&'_ self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let meta: Box<dyn DavMetaData> = Box::new(self.clone());
        Box::pin(async move { Ok(meta) })
    }
}

#[derive(Clone)]
pub struct DavFs {
    client: Client,
    dircache: Arc<RwLock<DirCache<String>>>,
}

impl DavFs {
    pub fn new(client: Client, dircache: Arc<RwLock<DirCache<String>>>) -> Self {
        Self { client, dircache }
    }

    pub fn new_boxed(client: Client, dircache: Arc<RwLock<DirCache<String>>>) -> Box<Self> {
        Box::new(Self::new(client, dircache))
    }
}

impl DavFileSystem for DavFs {
    fn open<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        options: OpenOptions,
    ) -> FsFuture<'a, Box<dyn FsDavFile>> {
        async move {
            if !options.read {
                return Err(FsError::Forbidden);
            }

            let result = search(
                self.client.clone(),
                path.as_url_string(),
                self.dircache.clone(),
            )
            .await?
            .ok_or(FsError::NotFound)?;

            let file = if let DirEntry::File(file) = result {
                file
            } else {
                return Err(FsError::Forbidden);
            };

            Ok(Box::new(DavFile::new(self.clone(), file)) as Box<dyn FsDavFile>)
        }
        .boxed()
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        async move {
            let result = search(
                self.client.clone(),
                path.as_url_string(),
                self.dircache.clone(),
            )
            .await?
            .ok_or(FsError::NotFound)?;

            let childrens = if let DirEntry::Folder(folder) = result {
                folder.children.into_values()
            } else {
                return Err(FsError::Forbidden);
            };

            let stream = futures_util::stream::iter(
                childrens.map(|entry| Ok::<Box<dyn DavDirEntry>, FsError>(Box::new(entry.clone()))),
            );
            Ok(Box::pin(stream) as FsStream<Box<dyn DavDirEntry>>)
        }
        .boxed()
    }

    fn metadata<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> FsFuture<'a, Box<dyn DavMetaData>> {
        async move {
            let result = search(
                self.client.clone(),
                path.as_url_string(),
                self.dircache.clone(),
            )
            .await?
            .ok_or(FsError::NotFound)?;

            result.metadata().await
        }
        .boxed()
    }
}

async fn search(
    client: Client,
    path: impl AsRef<str>,
    dircache: Arc<RwLock<DirCache<String>>>,
) -> Result<Option<DirEntry>> {
    let mut path = path.as_ref().to_string();
    path = if path.starts_with('/') {
        path
    } else {
        format!("/{}", path)
    };
    path = path
        .strip_suffix("/")
        .map(|s| s.to_string())
        .unwrap_or(path);
    let mut path = percent_decode_str(&path)
        .decode_utf8()
        .context("Invalid UTF-8 in percent-decoded URI path")?
        .to_string();

    let orig_path = path.clone();

    let (mut current_path, mut current_id) = {
        let dir_guard = dircache.read().await;
        loop {
            if let Some(id) = dir_guard.find_dir(&path) {
                break (path.clone(), id.to_owned());
            } else {
                let (parent_path, _) = path
                    .rsplit_once('/')
                    .context("Expected a path-like string starting with '/'")?;

                path = parent_path.to_owned()
            }
        }
    };

    if current_path == orig_path {
        let mut dir_guard = dircache.write().await;
        let mut contents = client.get_contents(current_id).await?;

        if let DirEntry::Folder(ref mut folder) = contents {
            let mut filtered_childs: HashMap<_, _> = HashMap::new();

            for child in folder.children.values() {
                // TODO Implement strategies for files that can't be read or streamed.
                // Currently skipped if inaccessible or frozen
                if let DirEntry::File(file) = child
                    && (!file.can_access || file.is_frozen)
                {
                    continue;
                }

                if let DirEntry::Folder(child_folder) = child {
                    dir_guard
                        .insert_dir(format!("/{}", child_folder.name), child_folder.code.clone());
                }

                filtered_childs.insert(child.id().to_string(), child.clone());
            }

            folder.children = filtered_childs;
        }

        return Ok(Some(contents));
    }

    let components = orig_path
        .strip_prefix(&current_path)
        .context("orig_path does not start with current_path")?
        .split('/')
        .filter(|s| !s.is_empty());

    for component in components {
        let result = client.get_contents(&current_id).await;
        let contents = match result {
            Ok(contents) => contents,
            Err(Error::NotFound) => return Ok(None),
            Err(e) => return Err(e),
        };

        let mut found_contents = None;
        {
            let mut dir_guard = dircache.write().await;
            if let DirEntry::Folder(ref folder) = contents {
                for child in folder.children.values() {
                    // TODO Implement strategies for files that can't be read or streamed.
                    // Currently skipped if inaccessible or frozen
                    if let DirEntry::File(file) = child
                        && (!file.can_access || file.is_frozen)
                    {
                        continue;
                    }

                    if child.name() == component {
                        found_contents = Some(child.clone())
                    }

                    if let DirEntry::Folder(child_folder) = child {
                        dir_guard.insert_dir(
                            format!("{current_path}/{}", child_folder.name),
                            child_folder.code.clone(),
                        );
                    }
                }
            }
        }
        if let Some(ref contents) = found_contents
            && orig_path == format!("{current_path}/{}", contents.name())
        {
            return Ok(found_contents);
        }

        if let Some(DirEntry::Folder(folder)) = found_contents {
            current_path = format!("{current_path}/{}", folder.name);
            current_id = folder.code
        } else {
            return Ok(None);
        }
    }

    Ok(None)
}
