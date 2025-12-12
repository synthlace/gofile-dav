use std::{
    collections::HashMap,
    io,
    pin::Pin,
    result::Result as StdResult,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::{Context, anyhow};
use bytes::{BufMut, Bytes, BytesMut};
use dav_server::{
    davpath::{DavPath, DavPathRef},
    fs::{
        DavDirEntry, DavFile as FsDavFile, DavFileSystem, DavMetaData, FsError, FsFuture, FsResult,
        FsStream, OpenOptions, ReadDirMeta,
    },
};
use futures_util::{FutureExt, Stream, StreamExt, TryFutureExt};
use percent_encoding::percent_decode_str;
use reqwest::{Method, header::RANGE, multipart::Part};
use tokio::{
    sync::{Mutex, RwLock, mpsc},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;

use super::{
    Client, DirCache,
    error::{Error, Result},
    model::{Attribute, Contents as DirEntry, FileEntry, FileUploaded, FolderEntry},
};

impl From<Error> for FsError {
    fn from(value: Error) -> Self {
        match value {
            Error::Io { source } => source.into(),
            Error::NotFound => FsError::NotFound,
            Error::Forbidden | Error::PasswordRequired | Error::PasswordWrong => FsError::Forbidden,
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

struct DavFileRead {
    fs: DavFs,
    position: u64,
    file: FileEntry,
    stream_buffer: Mutex<Option<StreamBuffer>>,
}

impl std::fmt::Debug for DavFileRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DavFile")
            .field("position", &self.position)
            .field("size", &self.file.size)
            .field("id", &self.file.id)
            .finish()
    }
}

impl DavFileRead {
    fn new(fs: DavFs, file: FileEntry) -> Self {
        Self {
            fs,
            position: 0,
            file,
            stream_buffer: Mutex::new(None),
        }
    }
}

impl FsDavFile for DavFileRead {
    fn read_bytes(&'_ mut self, count: usize) -> FsFuture<'_, bytes::Bytes> {
        async move {
            if count == 0 {
                return Ok(Bytes::new());
            }

            let client = &self.fs.client;
            let position = self.position;
            let url = self.file.link.clone();
            let range_header = format!("bytes={}-", position);

            if self.stream_buffer.get_mut().is_none() {
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

                *self.stream_buffer.get_mut() = Some(buffered_stream);
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

    fn write_buf(&mut self, _buf: Box<dyn bytes::Buf + Send>) -> FsFuture<'_, ()> {
        async { Err(FsError::NotImplemented) }.boxed()
    }

    fn write_bytes(&'_ mut self, _buf: bytes::Bytes) -> FsFuture<'_, ()> {
        async { Err(FsError::NotImplemented) }.boxed()
    }

    fn flush(&'_ mut self) -> FsFuture<'_, ()> {
        async { Err(FsError::NotImplemented) }.boxed()
    }
}

pub struct DavFileWrite {
    fs: DavFs,
    path: DavPath,
    sender: Option<mpsc::Sender<StdResult<Bytes, io::Error>>>,
    handle: Option<JoinHandle<Result<FileUploaded>>>,
}

impl std::fmt::Debug for DavFileWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DavFileWrite")
            .field("path", &self.path)
            .finish()
    }
}

impl DavFileWrite {
    fn new(fs: DavFs, path: DavPath) -> Self {
        Self {
            fs,
            path,
            sender: None,
            handle: None,
        }
    }
}

impl FsDavFile for DavFileWrite {
    fn metadata(&mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        async {
            let file_entry = self.fs.try_find_file(&self.path).await?;

            Ok(Box::new(DirEntry::File(file_entry)) as Box<dyn DavMetaData>)
        }
        .boxed()
    }

    fn write_buf(&mut self, mut buf: Box<dyn bytes::Buf + Send>) -> FsFuture<'_, ()> {
        async move {
            // Wasn't able to hit it

            while buf.has_remaining() {
                let chunk = buf.chunk();
                let chunk_bytes = Bytes::copy_from_slice(chunk);
                self.write_bytes(chunk_bytes).await?;
                buf.advance(chunk.len());
            }

            Ok(())
        }
        .boxed()
    }

    fn write_bytes(&'_ mut self, buf: bytes::Bytes) -> FsFuture<'_, ()> {
        async move {
            if self.sender.is_none() {
                let (tx, rx) = mpsc::channel::<StdResult<Bytes, io::Error>>(1);

                self.sender = Some(tx);

                let stream = ReceiverStream::new(rx);
                let body = reqwest::Body::wrap_stream(stream);

                let filename = self
                    .path
                    .file_name()
                    .map(String::from)
                    .ok_or(FsError::GeneralFailure)?;

                let file_part = Part::stream(body).file_name(filename);

                let folder_entry = self.fs.try_find_folder(&self.path.parent()).await?;

                let folder_id = folder_entry.id;

                let handle = tokio::spawn({
                    let client = self.fs.client.clone();
                    async move { client.upload_file(folder_id, file_part).await }
                });

                self.handle = Some(handle);
            }

            match self.sender.as_mut() {
                Some(sink) => {
                    sink.send(Ok(buf))
                        .await
                        .map_err(|_| FsError::GeneralFailure)?;
                }
                None => return Err(FsError::GeneralFailure),
            }

            Ok(())
        }
        .boxed()
    }

    fn read_bytes(&mut self, _count: usize) -> FsFuture<'_, bytes::Bytes> {
        async { Err(FsError::NotImplemented) }.boxed()
    }

    fn seek(&mut self, _pos: std::io::SeekFrom) -> FsFuture<'_, u64> {
        async { Err(FsError::NotImplemented) }.boxed()
    }

    fn flush(&mut self) -> FsFuture<'_, ()> {
        async move {
            // drop sender to close the stream
            self.sender.take();

            let filename = self
                .path
                .file_name()
                .map(String::from)
                .ok_or(FsError::GeneralFailure)?;

            let folder_entry = self.fs.try_find_folder(&self.path.parent()).await?;

            let mut to_delete: Vec<_> = folder_entry
                .children
                .values()
                .filter(|v| !v.is_dir() && v.name() == filename)
                .map(|v| v.id())
                .collect();

            let uploaded_id = if let Some(handle) = self.handle.take() {
                handle.await.map_err(io::Error::from)??.id
            } else {
                // create an empty file because write_bytes was never called, so its size is 0

                let file_part = Part::stream(Bytes::new()).file_name(filename.clone());
                self.fs
                    .client
                    .upload_file(folder_entry.id, file_part)
                    .await?
                    .id
            };

            to_delete.retain(|&v| v != uploaded_id);

            if !to_delete.is_empty() {
                self.fs.client.delete_contents(&to_delete).await?;
            }

            Ok(())
        }
        .boxed()
    }

    fn redirect_url(&mut self) -> FsFuture<'_, Option<String>> {
        async move { Ok(None) }.boxed()
    }
}

impl DavDirEntry for DirEntry {
    fn name(&self) -> Vec<u8> {
        self.name().as_bytes().to_vec()
    }

    fn metadata(&'_ self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        async move { Ok(Box::new(self.clone()) as Box<dyn DavMetaData>) }.boxed()
    }
}

#[derive(Clone)]
pub struct DavFs {
    client: Client,
    dircache: Arc<RwLock<DirCache<String>>>,
    write_enabled: bool,
}

impl DavFs {
    pub fn new(
        client: Client,
        dircache: Arc<RwLock<DirCache<String>>>,
        write_enabled: bool,
    ) -> Self {
        Self {
            client,
            dircache,
            write_enabled,
        }
    }

    pub fn new_boxed(
        client: Client,
        dircache: Arc<RwLock<DirCache<String>>>,
        write_enabled: bool,
    ) -> Box<Self> {
        Box::new(Self::new(client, dircache, write_enabled))
    }

    async fn remove(&self, path: &DavPath, remove_dir: bool) -> Result<()> {
        let contents = self.search(path).await?.ok_or(Error::NotFound)?;

        match (&contents, remove_dir) {
            (DirEntry::File(_), false) => {
                self.client.delete_contents(&[contents.id()]).await?;

                Ok(())
            }
            (DirEntry::Folder(folder_entry), true) => {
                if !folder_entry.children.is_empty() {
                    return Err(Error::Forbidden);
                }

                self.client.delete_contents(&[contents.id()]).await?;

                Ok(())
            }
            (DirEntry::File(file_entry), true) => {
                Err(anyhow!("expected to delete folder but found file {}", file_entry.id).into())
            }
            (DirEntry::Folder(folder_entry), false) => Err(anyhow!(
                "expected to delete file but found folder {}",
                folder_entry.id
            )
            .into()),
        }
    }

    async fn search(&self, path: &DavPathRef) -> Result<Option<DirEntry>> {
        let mut path = path.as_url_string();
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
            let dir_guard = self.dircache.read().await;
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
            let mut dir_guard = self.dircache.write().await;
            let mut contents = self.client.get_contents(current_id).await?;

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
                        dir_guard.insert_dir(
                            format!("/{}", child_folder.name),
                            child_folder.code.clone(),
                        );
                    }

                    filtered_childs.insert(child.id(), child.clone());
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
            let result = self.client.get_contents(&current_id).await;
            let contents = match result {
                Ok(contents) => contents,
                Err(Error::NotFound) => return Ok(None),
                Err(e) => return Err(e),
            };

            let mut found_contents = None;
            {
                let mut dir_guard = self.dircache.write().await;
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

    async fn try_find_folder(&self, path: &DavPathRef) -> Result<FolderEntry> {
        let contents = self.search(path).await?.ok_or(Error::NotFound)?;

        match contents {
            DirEntry::Folder(folder_entry) => Ok(folder_entry),
            DirEntry::File(_) => Err(anyhow!(
                "expected folder but found file at path {}",
                path.as_url_string()
            )
            .into()),
        }
    }

    async fn try_find_file(&self, path: &DavPathRef) -> Result<FileEntry> {
        let contents = self.search(path).await?.ok_or(Error::NotFound)?;

        match contents {
            DirEntry::File(file_entry) => Ok(file_entry),
            DirEntry::Folder(_) => Err(anyhow!(
                "expected file but found folder at path {}",
                path.as_url_string()
            )
            .into()),
        }
    }
}

impl DavFileSystem for DavFs {
    fn open<'a>(
        &'a self,
        path: &'a DavPath,
        options: OpenOptions,
    ) -> FsFuture<'a, Box<dyn FsDavFile>> {
        async move {
            if options.read {
                let result = self.search(path).await?.ok_or(FsError::NotFound)?;

                let file = if let DirEntry::File(file) = result {
                    file
                } else {
                    return Err(FsError::Forbidden);
                };

                return Ok(Box::new(DavFileRead::new(self.clone(), file)) as Box<dyn FsDavFile>);
            } else if self.write_enabled && options.write {
                if options.append {
                    return Err(FsError::NotImplemented);
                }

                return Ok(
                    Box::new(DavFileWrite::new(self.clone(), path.clone())) as Box<dyn FsDavFile>
                );
            }

            Err(FsError::Forbidden)
        }
        .boxed()
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        async move {
            let result = self.search(path).await?.ok_or(FsError::NotFound)?;

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

    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        async move {
            let result = self.search(path).await?.ok_or(FsError::NotFound)?;

            result.metadata().await
        }
        .boxed()
    }

    fn create_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            if !self.write_enabled {
                return Err(FsError::Forbidden);
            }

            let folder_name = path
                .file_name()
                .map(String::from)
                .ok_or(FsError::GeneralFailure)?;

            let parent_folder_entry = self.try_find_folder(&path.parent()).await?;

            let exist = parent_folder_entry
                .children
                .values()
                .any(|v| v.name() == folder_name);

            if exist {
                return Ok(());
            }

            self.client
                .create_folder(parent_folder_entry.id, folder_name)
                .await?;

            Ok(())
        }
        .boxed()
    }

    fn rename<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            if !self.write_enabled {
                return Err(FsError::Forbidden);
            }

            let name_from = from
                .file_name()
                .map(String::from)
                .ok_or(FsError::GeneralFailure)?;

            let name_to = to
                .file_name()
                .map(String::from)
                .ok_or(FsError::GeneralFailure)?;

            let parent_folder_from_entry = self.try_find_folder(&from.parent()).await?;

            let contents_from = parent_folder_from_entry
                .children
                .values()
                .find(|v| v.name() == name_from)
                .ok_or(FsError::GeneralFailure)?;

            let mut files_to_delete = Vec::with_capacity(2);

            if from.parent() == to.parent() {
                // share parent folder - simple rename

                let contents_to = parent_folder_from_entry
                    .children
                    .values()
                    .find(|v| v.name() == name_to);

                match (contents_from, contents_to) {
                    (DirEntry::File(_), Some(DirEntry::File(file_to))) => {
                        files_to_delete.push(file_to.id);
                    }
                    (DirEntry::Folder(_), Some(DirEntry::Folder(_))) => {
                        return Err(FsError::Exists);
                    }
                    (_, None) => (),
                    _ => return Err(FsError::GeneralFailure),
                }

                let new_attribute = Attribute::Name(&name_to);

                self.client
                    .update_attribute(contents_from.id(), new_attribute)
                    .await?;
            } else {
                let parent_folder_to_entry = self.try_find_folder(&to.parent()).await?;

                let contents_to = parent_folder_to_entry
                    .children
                    .values()
                    .find(|v| v.name() == name_to);

                match (contents_from, contents_to) {
                    (DirEntry::File(_), Some(DirEntry::File(file_to))) => {
                        files_to_delete.push(file_to.id);
                    }
                    (DirEntry::Folder(_), _) => {
                        // currently no move for folders
                        return Err(FsError::NotImplemented);
                    }
                    (_, None) => (),
                    _ => return Err(FsError::GeneralFailure),
                }

                // file to file move - simple copy + delete
                files_to_delete.push(contents_from.id());
                self.copy(from, to).await?;
            }

            if !files_to_delete.is_empty() {
                self.client.delete_contents(&files_to_delete).await?;
            }

            Ok(())
        }
        .boxed()
    }

    fn remove_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        self.remove(path, true).map_err(Into::into).boxed()
    }

    fn remove_file<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        self.remove(path, false).map_err(Into::into).boxed()
    }

    fn copy<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            if !self.write_enabled {
                return Err(FsError::Forbidden);
            }

            // https://github.com/messense/dav-server-rs/blob/4a1ae81485f46fe308c1eae5e474a0800fa68109/src/handle_gethead.rs#L29C1-L30C1
            const READ_BUF_SIZE: usize = 16384;

            let file_from = self.try_find_file(from).await?;

            let mut file_from = DavFileRead::new(self.clone(), file_from);
            let mut file_to = DavFileWrite::new(self.clone(), to.clone());

            let total_size = file_from.file.size;

            let mut pos: u64 = 0;

            while pos != total_size {
                let offset = std::io::SeekFrom::Start(pos);
                file_from.seek(offset).await?;

                let data = file_from.read_bytes(READ_BUF_SIZE).await?;
                let bytes_read = data.len();

                file_to.write_bytes(data).await?;

                pos += bytes_read as u64;
            }

            file_to.flush().await?;

            Ok(())
        }
        .boxed()
    }
}
