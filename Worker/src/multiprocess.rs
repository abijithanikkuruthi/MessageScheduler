#![allow(dead_code)]

use nix::unistd::{fork, ForkResult, Pid};
use nix::sys::wait::{waitpid, WaitStatus};
use std::process::exit;
use nix::sys::signal::kill;
use nix::sys::mman::{mmap, munmap, ProtFlags, MapFlags};
use nix::sys::memfd::{memfd_create, MemFdCreateFlag};
use nix::unistd::close;
use nix::errno::Errno;
use std::ffi::CString;
use std::os::raw::c_void;

use libc::{signal, SIGCHLD, SIG_IGN};

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

pub fn process<F, T>(func: F, arg: T) -> JoinHandle
where
    F: Fn(T)
{
    match unsafe { fork().unwrap() } {
        ForkResult::Parent { child: pid } => {
            // Avoiding zombie processes creation
            unsafe { signal(SIGCHLD, SIG_IGN); }
            JoinHandle { pid }
        }
        ForkResult::Child => {
            func(arg);
            exit(0);
        }
    }
}

pub struct JoinHandle {
    pub pid: Pid,
}

impl JoinHandle {
    pub fn join(self) -> CompletedProcess {
        CompletedProcess { status: waitpid(self.pid, None).unwrap_or(WaitStatus::StillAlive) }
    }
    pub fn kill(self) -> Result<(), Errno> {
        kill(self.pid, nix::sys::signal::Signal::SIGKILL)
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
        // Do not clean the memory if child process drops the value. Cleanup only when the process that called ::new() drops the value.
        SharedMemory { mem: self.mem, cleanup: false }
    }
}
