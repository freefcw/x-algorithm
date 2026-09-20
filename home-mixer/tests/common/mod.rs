//! Isolated `redis-server` fixture shared by the Redis integration tests.
//!
//! Each fixture starts its own server process on a private Unix socket under
//! `/tmp` and removes everything on drop, so test binaries and tests within a
//! binary never share state. Tests using it are `#[ignore]`d because they need
//! a local `redis-server`.

#![allow(dead_code)]

use home_mixer::clients::redis_feed_state_store::RedisFeedStateConfig;
use home_mixer::clients::uas_fetcher::RedisUserActionSequenceConfig;
use home_mixer::models::{ObjectId, UserId};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// A process-unique directory for fixtures that need their own socket.
pub fn fixture_directory(kind: &str) -> PathBuf {
    let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let directory = PathBuf::from(format!(
        "/tmp/home-mixer-{kind}-{}-{sequence}",
        std::process::id()
    ));
    fs::create_dir(&directory).unwrap_or_else(|error| {
        panic!(
            "failed to create isolated fixture directory {}: {error}",
            directory.display()
        )
    });
    directory
}

pub struct RedisFixture {
    child: Child,
    directory: PathBuf,
    socket: PathBuf,
    pub url: String,
}

impl RedisFixture {
    pub fn start() -> Self {
        let directory = fixture_directory("redis");
        let socket = directory.join("redis.sock");
        let url = unix_redis_url(&socket);
        let child = spawn_redis_server(&directory, &socket);
        let child = wait_until_ready(child, &url, &directory);

        Self {
            child,
            directory,
            socket,
            url,
        }
    }

    /// Persist the data set and stop the server, closing every client
    /// connection. Call `restart` to bring it back with the same data.
    pub fn stop_preserving_data(&mut self) {
        let mut control = self.connection();
        // The server closes the connection instead of replying, so the
        // command itself reports an error on success.
        let _ = redis::cmd("SHUTDOWN").arg("SAVE").query::<()>(&mut control);
        self.wait_for_exit();
    }

    /// Kill the server without saving, simulating an outage that loses the
    /// in-memory data set. Call `restart` to bring back an empty server.
    pub fn stop_losing_data(&mut self) {
        let _ = self.child.kill();
        self.wait_for_exit();
        let _ = fs::remove_file(self.directory.join("dump.rdb"));
    }

    fn wait_for_exit(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while self
            .child
            .try_wait()
            .expect("check isolated redis-server status")
            .is_none()
        {
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn restart(&mut self) {
        let _ = fs::remove_file(&self.socket);
        let child = spawn_redis_server(&self.directory, &self.socket);
        self.child = wait_until_ready(child, &self.url, &self.directory);
    }

    /// Close every client connection except the control connection, the way a
    /// proxy or a server-side idle timeout would.
    pub fn disconnect_clients(&self) {
        let mut control = self.connection();
        redis::cmd("CLIENT")
            .arg("KILL")
            .arg("TYPE")
            .arg("normal")
            .arg("SKIPME")
            .arg("yes")
            .query::<i64>(&mut control)
            .expect("disconnect adapter connections");
    }

    /// Block every command for `duration_ms`, simulating a stalled server.
    pub fn pause(&self, duration_ms: u64) {
        let mut control = self.connection();
        let response: String = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(duration_ms)
            .arg("ALL")
            .query(&mut control)
            .expect("pause isolated Redis");
        assert_eq!(response, "OK");
    }

    pub fn config(
        &self,
        key_prefix: &str,
        max_served_ids: usize,
        max_request_timestamps: usize,
    ) -> RedisFeedStateConfig {
        let mut config = RedisFeedStateConfig::new(self.url.clone());
        config.key_prefix = key_prefix.to_string();
        config.max_served_ids = max_served_ids;
        config.max_request_timestamps = max_request_timestamps;
        config.connect_timeout = Duration::from_millis(500);
        config.request_timeout = Duration::from_millis(500);
        config
    }

    pub fn uas_config(
        &self,
        key_prefix: &str,
        max_actions: usize,
    ) -> RedisUserActionSequenceConfig {
        let mut config = RedisUserActionSequenceConfig::new(self.url.clone());
        config.key_prefix = key_prefix.to_string();
        config.max_actions = max_actions;
        config.connect_timeout = Duration::from_millis(500);
        config.request_timeout = Duration::from_millis(500);
        config
    }

    pub fn connection(&self) -> redis::Connection {
        redis::Client::open(self.url.clone())
            .expect("valid fixture Redis URL")
            .get_connection()
            .expect("connect fixture control client")
    }

    pub fn key(&self, key_prefix: &str, user_id: UserId, suffix: &str) -> String {
        let external = ObjectId::from_u64_be_padded(user_id);
        format!("{key_prefix}:{{{external}}}:{suffix}")
    }
}

impl Drop for RedisFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_file(&self.socket);
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn redis_server_binary() -> PathBuf {
    let homebrew = PathBuf::from("/opt/homebrew/bin/redis-server");
    if homebrew.is_file() {
        homebrew
    } else {
        PathBuf::from("redis-server")
    }
}

fn spawn_redis_server(directory: &Path, socket: &Path) -> Child {
    let server = redis_server_binary();
    Command::new(&server)
        .arg("--port")
        .arg("0")
        .arg("--unixsocket")
        .arg(socket)
        .arg("--unixsocketperm")
        .arg("700")
        .arg("--save")
        .arg("")
        .arg("--appendonly")
        .arg("no")
        .arg("--dir")
        .arg(directory)
        .arg("--daemonize")
        .arg("no")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|error| {
            panic!(
                "failed to start isolated redis-server at {}: {error}",
                server.display()
            )
        })
}

fn wait_until_ready(mut child: Child, url: &str, directory: &Path) -> Child {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = child
            .try_wait()
            .expect("check isolated redis-server status")
        {
            let _ = fs::remove_dir_all(directory);
            panic!("isolated redis-server exited during startup: {status}");
        }
        if redis::Client::open(url)
            .and_then(|client| client.get_connection())
            .and_then(|mut connection| redis::cmd("PING").query::<String>(&mut connection))
            .is_ok()
        {
            return child;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = fs::remove_dir_all(directory);
            panic!("isolated redis-server did not become ready at {url}");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

pub fn unix_redis_url(socket: &Path) -> String {
    format!("redis+unix://{}", socket.display())
}

pub fn object_id(value: &str) -> ObjectId {
    ObjectId::parse(value).expect("valid 96-bit ObjectId fixture")
}

/// A three-node `redis-server --cluster-enabled yes` fixture on loopback TCP
/// ports. Cluster mode cannot run over Unix sockets (nodes announce TCP
/// addresses to each other), so this uses private high ports; tests using it
/// are `#[ignore]d and need both `redis-server` and `redis-cli`.
pub struct RedisClusterFixture {
    children: Vec<Child>,
    directory: PathBuf,
    ports: [u16; 3],
}

impl RedisClusterFixture {
    pub fn start() -> Self {
        let directory = fixture_directory("redis-cluster");
        // Ephemeral free ports with retry: the feed-state and UAS test
        // binaries run in parallel and each brings its own cluster, so fixed
        // ports would collide, and a reserved-then-dropped port can be
        // stolen in the window before redis-server binds it.
        let mut children = Vec::new();
        let mut ports = Vec::new();
        for index in 0..3 {
            let (port, child) = spawn_cluster_node_with_retry(&directory, index);
            ports.push(port);
            children.push(child);
        }
        // All nodes are already running while the cluster is created.
        let cli = redis_cli_binary();
        let mut create = Command::new(&cli);
        create.arg("--cluster").arg("create");
        for &port in &ports {
            create.arg(format!("127.0.0.1:{port}"));
        }
        create
            .arg("--cluster-replicas")
            .arg("0")
            .arg("--cluster-yes");
        let create_output = create.output().unwrap_or_else(|error| {
            panic!("failed to run redis-cli at {}: {error}", cli.display())
        });
        if !create_output.status.success()
            || String::from_utf8_lossy(&create_output.stderr).contains("[ERR]")
        {
            // Kill the nodes before panicking: the fixture was never
            // constructed, so its Drop would not run.
            for child in &mut children {
                let _ = child.kill();
                let _ = child.wait();
            }
            let _ = fs::remove_dir_all(&directory);
            panic!(
                "redis-cli --cluster create failed:\n{}\n{}",
                String::from_utf8_lossy(&create_output.stdout),
                String::from_utf8_lossy(&create_output.stderr)
            );
        }

        Self {
            children,
            directory,
            ports: [ports[0], ports[1], ports[2]],
        }
    }

    pub fn seed_urls(&self) -> Vec<String> {
        self.ports.iter().map(|&p| tcp_redis_url(p)).collect()
    }

    /// Feed-state config pointed at the cluster (no single URL).
    pub fn feed_config(
        &self,
        key_prefix: &str,
        max_served_ids: usize,
        max_request_timestamps: usize,
    ) -> RedisFeedStateConfig {
        let mut config = RedisFeedStateConfig::new(String::new());
        config.cluster_urls = Some(self.seed_urls());
        config.key_prefix = key_prefix.to_string();
        config.max_served_ids = max_served_ids;
        config.max_request_timestamps = max_request_timestamps;
        config.connect_timeout = Duration::from_millis(2_000);
        config.request_timeout = Duration::from_millis(2_000);
        config
    }

    /// UAS config pointed at the cluster (no single URL).
    pub fn uas_config(
        &self,
        key_prefix: &str,
        max_actions: usize,
    ) -> RedisUserActionSequenceConfig {
        let mut config = RedisUserActionSequenceConfig::new(String::new());
        config.cluster_urls = Some(self.seed_urls());
        config.key_prefix = key_prefix.to_string();
        config.max_actions = max_actions;
        config.connect_timeout = Duration::from_millis(2_000);
        config.request_timeout = Duration::from_millis(2_000);
        config
    }
}

impl Drop for RedisClusterFixture {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn tcp_redis_url(port: u16) -> String {
    format!("redis://127.0.0.1:{port}/")
}

fn reserve_loopback_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve a loopback port")
        .local_addr()
        .expect("read the reserved port")
        .port()
}

/// Spawn one cluster node, retrying on a fresh port when the node exits
/// during startup (its reserved port was taken in the bind window).
fn spawn_cluster_node_with_retry(directory: &Path, index: usize) -> (u16, Child) {
    for _ in 0..5 {
        let port = reserve_loopback_port();
        let server = redis_server_binary();
        let mut child = Command::new(&server)
            .arg("--port")
            .arg(port.to_string())
            // The cluster bus defaults to port+10000, which overflows the
            // 16-bit range for macOS ephemeral ports; pin it adjacent
            // instead. A bind failure on either port just triggers a retry.
            .arg("--cluster-port")
            .arg((port + 1).to_string())
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--cluster-enabled")
            .arg("yes")
            .arg("--cluster-config-file")
            .arg(format!("nodes-{index}-{}.conf", port))
            .arg("--cluster-node-timeout")
            .arg("5000")
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .arg("--dir")
            .arg(directory)
            .arg("--daemonize")
            .arg("no")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|error| {
                panic!(
                    "failed to start cluster redis-server at {}: {error}",
                    server.display()
                )
            });
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = child.try_wait().expect("check cluster redis-server status") {
                eprintln!("cluster node on port {port} exited during startup ({status}); retrying");
                break;
            }
            if redis::Client::open(tcp_redis_url(port))
                .and_then(|client| client.get_connection())
                .and_then(|mut connection| redis::cmd("PING").query::<String>(&mut connection))
                .is_ok()
            {
                return (port, child);
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
    panic!("could not start cluster node {index} after retries");
}

fn redis_cli_binary() -> PathBuf {
    let homebrew = PathBuf::from("/opt/homebrew/bin/redis-cli");
    if homebrew.is_file() {
        homebrew
    } else {
        PathBuf::from("redis-cli")
    }
}
