use nix::unistd::{fork, ForkResult, Pid};
use nix::sys::wait::{waitpid, WaitStatus};
use std::process::exit;
use nix::sys::mman::{mmap, munmap, ProtFlags, MapFlags};
use nix::sys::memfd::{memfd_create, MemFdCreateFlag};
use nix::unistd::close;
use nix::errno::Errno;
use std::ffi::CString;
use std::os::raw::c_void;

pub fn spawn<F, T>(func: F, args: &[T]) -> JoinHandle
where
    F: Fn(&[T]) + Send + 'static,
{
    match unsafe { fork().unwrap() } {
        ForkResult::Parent { child: pid } => {
            JoinHandle { pid }
        }
        ForkResult::Child => {
            func(args);
            exit(0);
        }
    }
}

pub fn process<F>(func: F) -> JoinHandle
where
    F: Fn(),
{
    match unsafe { fork().unwrap() } {
        ForkResult::Parent { child: pid } => {
            JoinHandle { pid }
        }
        ForkResult::Child => {
            func();
            exit(0);
        }
    }
}

pub fn worker_process<F, T>(func: F, arg1: T) -> JoinHandle
where
    F: Fn(T)// + Send + 'static,
{
    match unsafe { fork().unwrap() } {
        ForkResult::Parent { child: pid } => {
            JoinHandle { pid }
        }
        ForkResult::Child => {
            func(arg1);
            exit(0);
        }
    }
}

pub struct JoinHandle {
    pid: Pid,
}

impl JoinHandle {
    pub fn join(self) -> CompletedProcess {
        CompletedProcess { status: waitpid(self.pid, None).unwrap_or(WaitStatus::StillAlive) }
    }
}

pub struct CompletedProcess {
    status: WaitStatus,
}

impl CompletedProcess {
    pub fn success(&self) -> bool {
        self.exitcode() == Some(0)
    }

    pub fn exitcode(&self) -> Option<i32> {
        match self.status {
            WaitStatus::Exited(_, code) => Some(code),
            _ => None,
        }
    }
}

pub struct SharedMemory<T> {
    mem: *mut T,
    cleanup: bool,
}

impl<T> SharedMemory<T> {
    pub fn new(data: T) -> Result<Self, Errno> {
        let addr = std::ptr::null_mut();
        let size = std::mem::size_of::<T>();
        let prot = ProtFlags::PROT_READ | ProtFlags::PROT_WRITE;
        let flags = MapFlags::MAP_SHARED | MapFlags::MAP_ANONYMOUS;
        let fd = memfd_create(&CString::new("memfd").unwrap(), MemFdCreateFlag::empty())?;
        let offset = 0;
        let mem = unsafe { mmap(addr, size, prot, flags, fd, offset)?.cast::<T>() };
        close(fd).unwrap();
        unsafe { *mem = data; }
        Ok(SharedMemory { mem, cleanup: true })
    }

    pub fn get(&self) -> T {
        unsafe { self.mem.read() }
    }

    pub fn set(&self, data: T) {
        unsafe { *self.mem = data; }
    }
}

impl<T> Drop for SharedMemory<T> {
    fn drop(&mut self) {
        if self.cleanup {
            let size = std::mem::size_of::<T>();
            unsafe { munmap(self.mem.cast::<c_void>(), size) }.unwrap();
        }
    }
}

impl<T> Clone for SharedMemory<T> {
    fn clone(&self) -> Self {
        SharedMemory { mem: self.mem, cleanup: false }
    }
}