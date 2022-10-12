// TODO: use use try_from everywhere

use std::any::Any;
use std::io::{IoSlice, IoSliceMut, SeekFrom, Write};
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Context;
use tokio::sync::RwLock;
use wasi_common::file::*;
use wasi_common::{Error, ErrorExt, SystemTimeSpec, WasiFile};

/// A file that is backed by a memory buffer.
///
/// This structure contains the data of the file. It does not, however,
/// contain the state that is related to an open file descriptor. For
/// that, look at the [`OpenMemoryFile`] structure.
pub struct MemoryFile {
    device_id: u64,
    inode: u64,
    nlink: u64,
    contents: Vec<u8>,
    flags: FdFlags,
    access_time: SystemTime,
    modify_time: SystemTime,
    create_time: SystemTime,
    _read: bool,
    _write: bool,
}

/// An open file that is backed by a memory buffer.
///
/// This structure contains the state that is related to an open file as well
/// as a reference to the file contents ([`MemoryFile`]) itself.
pub struct OpenMemoryFile {
    file: Arc<RwLock<MemoryFile>>,
    position: usize,
    _read: bool,
    _write: bool,
}

#[async_trait::async_trait]
impl WasiFile for OpenMemoryFile {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn get_filetype(&mut self) -> Result<FileType, Error> {
        Ok(FileType::RegularFile)
    }

    async fn datasync(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn get_fdflags(&mut self) -> Result<FdFlags, Error> {
        Ok(self.file.read().await.flags)
    }

    async fn set_fdflags(&mut self, flags: FdFlags) -> Result<(), Error> {
        self.file.write().await.flags = flags;
        Ok(())
    }

    async fn get_filestat(&mut self) -> Result<Filestat, Error> {
        let file = self.file.read().await;
        Ok(Filestat {
            filetype: FileType::SocketDgram,
            device_id: file.device_id,
            inode: file.inode,
            nlink: file.nlink,
            size: file.contents.len() as u64,
            atim: Some(file.access_time),
            mtim: Some(file.modify_time),
            ctim: Some(file.create_time),
        })
    }

    // TODO: Difference from allocate()?
    async fn set_filestat_size(&mut self, size: u64) -> Result<(), Error> {
        let mut file = self.file.write().await;
        file.contents.resize(size as usize, 0);
        Ok(())
    }

    async fn advise(&mut self, _offset: u64, _len: u64, _advice: Advice) -> Result<(), Error> {
        Ok(())
    }

    // TODO: Difference from set_filestat_size()?
    async fn allocate(&mut self, _offset: u64, size: u64) -> Result<(), Error> {
        let mut file = self.file.write().await;
        file.contents.resize(size as usize, 0);
        Ok(())
    }

    async fn set_times(
        &mut self,
        atime: Option<SystemTimeSpec>,
        mtime: Option<SystemTimeSpec>,
    ) -> Result<(), Error> {
        let mut file = self.file.write().await;

        match atime {
            Some(SystemTimeSpec::SymbolicNow) => file.access_time = SystemTime::now(),
            Some(SystemTimeSpec::Absolute(time)) => file.access_time = time.into_std(),
            None => {}
        }

        match mtime {
            Some(SystemTimeSpec::SymbolicNow) => file.modify_time = SystemTime::now(),
            Some(SystemTimeSpec::Absolute(time)) => file.modify_time = time.into_std(),
            None => {}
        }

        Ok(())
    }

    async fn read_vectored<'a>(&mut self, bufs: &mut [IoSliceMut<'a>]) -> Result<u64, Error> {
        let result = self.read_vectored_at(bufs, self.position as u64).await;

        if let Ok(read) = result {
            self.position += read as usize;
        }

        result
    }

    async fn read_vectored_at<'a>(
        &mut self,
        bufs: &mut [IoSliceMut<'a>],
        offset: u64,
    ) -> Result<u64, Error> {
        let offset = usize::try_from(offset).map_err(|_| Error::invalid_argument())?;
        let n = bufs.iter().map(|b| b.len()).sum();
        let mut file = self.file.write().await;

        if file.contents.len() < offset + n {
            file.contents.resize(offset + n, 0);
        }

        let content = &mut file.contents[offset..][..n];

        for buf in bufs {
            // TODO: make sure all bytes are written
            buf.deref_mut()
                .write(content)
                .with_context(|| "read failed")?;
        }

        Ok(n as u64)
    }

    async fn write_vectored<'a>(&mut self, bufs: &[IoSlice<'a>]) -> Result<u64, Error> {
        let result = self.write_vectored_at(bufs, self.position as u64).await;

        if let Ok(read) = result {
            self.position += read as usize;
        }

        result
    }

    async fn write_vectored_at<'a>(
        &mut self,
        bufs: &[IoSlice<'a>],
        offset: u64,
    ) -> Result<u64, Error> {
        let offset = usize::try_from(offset).map_err(|_| Error::invalid_argument())?;
        let n = bufs.iter().map(|b| b.len()).sum();
        let mut file = self.file.write().await;

        if file.contents.len() < offset + n {
            file.contents.resize(offset + n, 0);
        }

        let mut content = &mut file.contents[offset..][..n];

        for buf in bufs {
            content.write_all(buf)?;
        }

        Ok(n as u64)
    }

    async fn seek(&mut self, pos: SeekFrom) -> Result<u64, Error> {
        let file = self.file.read().await;

        match pos {
            SeekFrom::Start(offset) => {
                self.position = offset.try_into().map_err(|_| Error::invalid_argument())?;
            }
            SeekFrom::Current(offset) => {
                self.position = self
                    .position
                    .checked_add(offset as usize)
                    .ok_or_else(Error::invalid_argument)?;
            }
            SeekFrom::End(offset) => {
                self.position = file
                    .contents
                    .len()
                    .checked_sub(offset as usize)
                    .ok_or_else(Error::invalid_argument)?;
            }
        }

        Ok(self.position as u64)
    }

    async fn peek(&mut self, buf: &mut [u8]) -> Result<u64, Error> {
        let mut slices = [IoSliceMut::new(buf)];
        let n = self.read_vectored(&mut slices).await?;
        self.position -= n as usize;
        Ok(n)
    }

    async fn num_ready_bytes(&self) -> Result<u64, Error> {
        let file = self.file.read().await;
        Ok(file.contents.len() as u64 - self.position as u64)
    }

    async fn readable(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn writable(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[tokio::test]
async fn io_test() {
    let contents_before = "Hello World".as_bytes().to_vec();
    let  write_bytes = ", Bob".as_bytes().to_vec();
    let contents_after = "Hello World, Bob".as_bytes().to_vec();
    let now = SystemTime::now();
    let file = MemoryFile {
        device_id: 0,
        inode: 0,
        nlink: 0,
        contents: contents_before.clone(),
        flags: FdFlags::empty(),
        access_time: now,
        modify_time: now,
        create_time: now,
        _read: true,
        _write: true,
    };
    let mut file = OpenMemoryFile {
        file: Arc::new(RwLock::new(file)),
        position: 0,
        _read: true,
        _write: true,
    };

    let mut read_buffer = vec![];
    assert_eq!(
        file.read_vectored(&mut [IoSliceMut::new(&mut read_buffer)])
            .await
            .unwrap() as usize,
        read_buffer.len()
    );
    read_buffer.resize(contents_before.len(), 0);
    assert_eq!(
        file.read_vectored(&mut [IoSliceMut::new(&mut read_buffer)])
            .await
            .unwrap() as usize,
        read_buffer.len()
    );
    assert_eq!(read_buffer, contents_before);
    assert_eq!(
        file.write_vectored_at(&[IoSlice::new(&write_bytes)], read_buffer.len() as u64)
            .await
            .unwrap() as usize,
        write_bytes.len()
    );
    read_buffer.resize(contents_after.len(), 0);
    assert_eq!(
        file.read_vectored_at(&mut [IoSliceMut::new(&mut read_buffer)], 0)
            .await
            .unwrap() as usize,
        read_buffer.len()
    );
    assert_eq!(read_buffer, contents_after);
}
