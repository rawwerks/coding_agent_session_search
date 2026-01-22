//! Local preview server for Pages exports.
//!
//! Provides a local HTTP server to preview exported archives before deployment.
//! Features:
//! - Static file serving with correct MIME types
//! - COOP/COEP headers for full WebCrypto functionality
//! - Auto-open browser on start
//! - Graceful shutdown via Ctrl+C

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::watch;

/// Error type for preview server operations.
#[derive(Debug)]
pub enum PreviewError {
    /// Failed to bind to the specified port.
    BindFailed { port: u16, source: std::io::Error },
    /// The site directory does not exist.
    SiteDirectoryNotFound(PathBuf),
    /// Failed to read a file.
    FileReadError { path: PathBuf, source: std::io::Error },
    /// Failed to open browser.
    BrowserOpenFailed(String),
    /// Server error.
    ServerError(String),
}

impl std::fmt::Display for PreviewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BindFailed { port, source } => {
                write!(f, "Failed to bind to port {}: {}", port, source)
            }
            Self::SiteDirectoryNotFound(path) => {
                write!(f, "Site directory not found: {}", path.display())
            }
            Self::FileReadError { path, source } => {
                write!(f, "Failed to read file {}: {}", path.display(), source)
            }
            Self::BrowserOpenFailed(msg) => {
                write!(f, "Failed to open browser: {}", msg)
            }
            Self::ServerError(msg) => {
                write!(f, "Server error: {}", msg)
            }
        }
    }
}

impl std::error::Error for PreviewError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BindFailed { source, .. } => Some(source),
            Self::FileReadError { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Configuration for the preview server.
#[derive(Debug, Clone)]
pub struct PreviewConfig {
    /// Directory containing the site to serve.
    pub site_dir: PathBuf,
    /// Port to listen on.
    pub port: u16,
    /// Whether to automatically open a browser.
    pub open_browser: bool,
}

impl Default for PreviewConfig {
    fn default() -> Self {
        Self {
            site_dir: PathBuf::from("."),
            port: 8080,
            open_browser: true,
        }
    }
}

/// Guess MIME type from file extension.
fn guess_mime_type(path: &std::path::Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") | Some("htm") => "text/html; charset=utf-8",
        Some("js") | Some("mjs") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("wasm") => "application/wasm",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        Some("txt") => "text/plain; charset=utf-8",
        Some("xml") => "application/xml",
        Some("pdf") => "application/pdf",
        Some("bin") => "application/octet-stream",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("ttf") => "font/ttf",
        Some("otf") => "font/otf",
        Some("eot") => "application/vnd.ms-fontobject",
        Some("mp4") => "video/mp4",
        Some("webm") => "video/webm",
        Some("mp3") => "audio/mpeg",
        Some("ogg") => "audio/ogg",
        Some("wav") => "audio/wav",
        Some("zip") => "application/zip",
        Some("gz") => "application/gzip",
        Some("tar") => "application/x-tar",
        _ => "application/octet-stream",
    }
}

/// Build an HTTP response with the given status code, content type, and body.
fn build_response(status: u16, content_type: &str, body: Vec<u8>) -> Vec<u8> {
    let status_text = match status {
        200 => "OK",
        304 => "Not Modified",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Unknown",
    };

    let headers = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: {}\r\n\
         Content-Length: {}\r\n\
         Cross-Origin-Opener-Policy: same-origin\r\n\
         Cross-Origin-Embedder-Policy: require-corp\r\n\
         Cross-Origin-Resource-Policy: same-origin\r\n\
         Cache-Control: no-cache\r\n\
         Connection: close\r\n\
         \r\n",
        status, status_text, content_type, body.len()
    );

    let mut response = headers.into_bytes();
    response.extend(body);
    response
}

/// Handle a single HTTP request.
async fn handle_request(
    site_dir: &std::path::Path,
    request: &str,
) -> Vec<u8> {
    // Parse the request line
    let request_line = request.lines().next().unwrap_or("");
    let parts: Vec<&str> = request_line.split_whitespace().collect();

    if parts.len() < 2 {
        return build_response(400, "text/plain", b"Bad Request".to_vec());
    }

    let method = parts[0];
    let path = parts[1];

    // Only support GET and HEAD
    if method != "GET" && method != "HEAD" {
        return build_response(400, "text/plain", b"Method Not Allowed".to_vec());
    }

    // Decode URL and sanitize path
    let decoded_path = urlencoding::decode(path).unwrap_or_else(|_| path.into());
    let request_path = decoded_path.trim_start_matches('/');

    // Prevent directory traversal
    if request_path.contains("..") {
        return build_response(400, "text/plain", b"Invalid Path".to_vec());
    }

    // Determine the file path
    let file_path = if request_path.is_empty() || request_path == "/" {
        site_dir.join("index.html")
    } else {
        site_dir.join(request_path)
    };

    // Canonicalize to prevent path traversal
    let canonical = match file_path.canonicalize() {
        Ok(p) => p,
        Err(_) => {
            // Try with index.html if it's a directory
            let with_index = file_path.join("index.html");
            match with_index.canonicalize() {
                Ok(p) => p,
                Err(_) => {
                    return build_response(404, "text/plain", b"Not Found".to_vec());
                }
            }
        }
    };

    // Ensure the path is within the site directory
    let site_canonical = match site_dir.canonicalize() {
        Ok(p) => p,
        Err(_) => {
            return build_response(500, "text/plain", b"Internal Server Error".to_vec());
        }
    };

    if !canonical.starts_with(&site_canonical) {
        return build_response(400, "text/plain", b"Invalid Path".to_vec());
    }

    // Read the file
    match tokio::fs::read(&canonical).await {
        Ok(contents) => {
            let mime = guess_mime_type(&canonical);
            if method == "HEAD" {
                // For HEAD requests, return headers only
                build_response(200, mime, vec![])
            } else {
                build_response(200, mime, contents)
            }
        }
        Err(_) => {
            build_response(404, "text/plain", b"Not Found".to_vec())
        }
    }
}

/// Start the preview server.
///
/// This function will block until the server is shut down (via Ctrl+C).
///
/// # Arguments
///
/// * `config` - Server configuration
///
/// # Returns
///
/// Returns `Ok(())` on graceful shutdown, or an error if the server fails to start.
pub async fn start_preview_server(config: PreviewConfig) -> Result<(), PreviewError> {
    // Verify site directory exists
    if !config.site_dir.exists() {
        return Err(PreviewError::SiteDirectoryNotFound(config.site_dir));
    }

    let site_dir = Arc::new(
        config
            .site_dir
            .canonicalize()
            .map_err(|e| PreviewError::SiteDirectoryNotFound(config.site_dir.clone()))?,
    );

    // Bind to the address
    let addr = SocketAddr::from(([127, 0, 0, 1], config.port));
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| PreviewError::BindFailed {
            port: config.port,
            source: e,
        })?;

    // Create shutdown signal channel
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // Print startup message
    eprintln!();
    eprintln!(
        "\x1b[1;32m{}\x1b[0m Preview server running at \x1b[1;36mhttp://localhost:{}\x1b[0m",
        "\u{1F310}",  // Globe emoji
        config.port
    );
    eprintln!(
        "   Serving: \x1b[33m{}\x1b[0m",
        site_dir.display()
    );
    eprintln!("   Press \x1b[1mCtrl+C\x1b[0m to stop");
    eprintln!();

    // Open browser if requested
    if config.open_browser {
        let url = format!("http://localhost:{}", config.port);
        if let Err(e) = open_browser(&url) {
            eprintln!(
                "\x1b[33mWarning:\x1b[0m Could not open browser: {}",
                e
            );
            eprintln!("   Please open \x1b[1;36m{}\x1b[0m manually", url);
        }
    }

    // Setup Ctrl+C handler
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            eprintln!();
            eprintln!("\x1b[33mShutting down preview server...\x1b[0m");
            let _ = shutdown_tx_clone.send(true);
        }
    });

    // Main server loop
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((mut stream, _addr)) => {
                        let site_dir = Arc::clone(&site_dir);
                        tokio::spawn(async move {
                            use tokio::io::{AsyncReadExt, AsyncWriteExt};

                            // Read the request
                            let mut buf = vec![0u8; 8192];
                            let n = match stream.read(&mut buf).await {
                                Ok(n) if n > 0 => n,
                                _ => return,
                            };

                            let request = String::from_utf8_lossy(&buf[..n]);

                            // Handle the request
                            let response = handle_request(&site_dir, &request).await;

                            // Send the response
                            let _ = stream.write_all(&response).await;
                            let _ = stream.shutdown().await;
                        });
                    }
                    Err(e) => {
                        eprintln!("Accept error: {}", e);
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }

    eprintln!("\x1b[32mPreview server stopped.\x1b[0m");
    Ok(())
}

/// Open the default browser to the given URL.
fn open_browser(url: &str) -> Result<(), PreviewError> {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open")
            .arg(url)
            .spawn()
            .map_err(|e| PreviewError::BrowserOpenFailed(e.to_string()))?;
    }

    #[cfg(target_os = "linux")]
    {
        // Try xdg-open first, fall back to common browsers
        let browsers = ["xdg-open", "firefox", "chromium", "google-chrome", "x-www-browser"];
        let mut opened = false;

        for browser in browsers {
            if std::process::Command::new(browser)
                .arg(url)
                .spawn()
                .is_ok()
            {
                opened = true;
                break;
            }
        }

        if !opened {
            return Err(PreviewError::BrowserOpenFailed(
                "No browser found. Install xdg-open or a web browser.".to_string(),
            ));
        }
    }

    #[cfg(target_os = "windows")]
    {
        std::process::Command::new("cmd")
            .args(["/C", "start", "", url])
            .spawn()
            .map_err(|e| PreviewError::BrowserOpenFailed(e.to_string()))?;
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        return Err(PreviewError::BrowserOpenFailed(
            "Unsupported platform for auto-open".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guess_mime_type() {
        assert_eq!(
            guess_mime_type(std::path::Path::new("index.html")),
            "text/html; charset=utf-8"
        );
        assert_eq!(
            guess_mime_type(std::path::Path::new("app.js")),
            "application/javascript; charset=utf-8"
        );
        assert_eq!(
            guess_mime_type(std::path::Path::new("styles.css")),
            "text/css; charset=utf-8"
        );
        assert_eq!(
            guess_mime_type(std::path::Path::new("data.json")),
            "application/json; charset=utf-8"
        );
        assert_eq!(
            guess_mime_type(std::path::Path::new("module.wasm")),
            "application/wasm"
        );
        assert_eq!(
            guess_mime_type(std::path::Path::new("image.png")),
            "image/png"
        );
        assert_eq!(
            guess_mime_type(std::path::Path::new("unknown")),
            "application/octet-stream"
        );
    }

    #[test]
    fn test_preview_config_default() {
        let config = PreviewConfig::default();
        assert_eq!(config.port, 8080);
        assert!(config.open_browser);
    }

    #[test]
    fn test_build_response_headers() {
        let response = build_response(200, "text/html", b"<html></html>".to_vec());
        let response_str = String::from_utf8_lossy(&response);

        assert!(response_str.contains("HTTP/1.1 200 OK"));
        assert!(response_str.contains("Content-Type: text/html"));
        assert!(response_str.contains("Cross-Origin-Opener-Policy: same-origin"));
        assert!(response_str.contains("Cross-Origin-Embedder-Policy: require-corp"));
        assert!(response_str.contains("Cross-Origin-Resource-Policy: same-origin"));
    }

    #[tokio::test]
    async fn test_handle_request_bad_method() {
        let site_dir = std::path::Path::new("/tmp");
        let response = handle_request(site_dir, "POST / HTTP/1.1\r\n").await;
        let response_str = String::from_utf8_lossy(&response);
        assert!(response_str.contains("400") || response_str.contains("Method Not Allowed"));
    }

    #[tokio::test]
    async fn test_handle_request_bad_path() {
        let site_dir = std::path::Path::new("/tmp");
        let response = handle_request(site_dir, "GET /../etc/passwd HTTP/1.1\r\n").await;
        let response_str = String::from_utf8_lossy(&response);
        assert!(response_str.contains("400") || response_str.contains("Invalid"));
    }
}
