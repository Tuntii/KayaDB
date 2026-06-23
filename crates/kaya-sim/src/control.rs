use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

pub struct NodeController {
    node_id: u64,
    child: Child,
    pid: u32,
    data_dir: PathBuf,
}

impl NodeController {
    /// Spawn a new kayadb-server process.
    pub fn spawn(
        node_id: u64,
        binary_path: &Path,
        data_dir: &Path,
        client_port: u16,
        raft_port: u16,
        peers: &[(u64, String, String)],
    ) -> std::io::Result<Self> {
        let mut cmd = Command::new(binary_path);
        cmd.arg("--node-id")
            .arg(node_id.to_string())
            .arg("--raft-addr")
            .arg(format!("127.0.0.1:{}", raft_port))
            .arg("--client-addr")
            .arg(format!("127.0.0.1:{}", client_port))
            .arg("--data")
            .arg(data_dir.as_os_str())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        for (peer_id, peer_raft, peer_client) in peers {
            cmd.arg("--peer")
                .arg(format!("{}={},{}", peer_id, peer_raft, peer_client));
        }

        let child = cmd.spawn()?;
        let pid = child.id();

        Ok(Self {
            node_id,
            child,
            pid,
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Stop the child process forcefully and reap it immediately to free ports.
    pub fn stop(&mut self) -> std::io::Result<()> {
        let _ = self.child.kill();
        let _ = self.child.wait();
        Ok(())
    }

    /// Pause (suspend) the node.
    pub fn pause(&self) -> std::io::Result<()> {
        change_process_threads_state(self.pid, true)
    }

    /// Resume (unsuspend) the node.
    pub fn resume(&self) -> std::io::Result<()> {
        change_process_threads_state(self.pid, false)
    }

    /// Get the node's numeric identity.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Get the process ID of the child.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Get the data directory of this node.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

impl Drop for NodeController {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

// ── Platform-Specific Suspension Logic ────────────────────────────────────────

#[cfg(target_os = "windows")]
fn change_process_threads_state(pid: u32, suspend: bool) -> std::io::Result<()> {
    use std::io::Error;
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
    };
    use windows_sys::Win32::System::Threading::{
        OpenThread, ResumeThread, SuspendThread, THREAD_SUSPEND_RESUME,
    };

    unsafe {
        let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(Error::last_os_error());
        }

        let mut entry: THREADENTRY32 = std::mem::zeroed();
        entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;

        if Thread32First(snapshot, &mut entry) == 0 {
            CloseHandle(snapshot);
            return Err(Error::last_os_error());
        }

        loop {
            if entry.th32OwnerProcessID == pid {
                let thread_handle = OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID);
                if !thread_handle.is_null() {
                    if suspend {
                        SuspendThread(thread_handle);
                    } else {
                        ResumeThread(thread_handle);
                    }
                    CloseHandle(thread_handle);
                }
            }

            if Thread32Next(snapshot, &mut entry) == 0 {
                break;
            }
        }

        CloseHandle(snapshot);
    }
    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn change_process_threads_state(pid: u32, suspend: bool) -> std::io::Result<()> {
    let sig = if suspend {
        libc::SIGSTOP
    } else {
        libc::SIGCONT
    };
    let res = unsafe { libc::kill(pid as libc::pid_t, sig) };
    if res != 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}
